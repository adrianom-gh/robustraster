import uuid
import multiprocessing
import psutil
import socket
import atexit
from typing import Optional, Dict, Any, List

import docker
from dask.distributed import Client
import time

class DDClusterManager:
    """
    Docker-backed Dask cluster manager.
    Mirrors the public API of `DaskClusterManager`.
    """
    def __init__(
        self,
        dask_client: Client = None,
        docker_image: Optional[str] = None,
        network_name: str = "dask-network",
        **kwargs,
    ) -> None:
        self.dask_client = dask_client
        self.docker_image = docker_image
        self.network_name = kwargs.get("network_name", network_name)
        self.docker_client = docker.from_env()
        self.scheduler = None
        self.workers = []
        self._container_prefix = f"dask-{uuid.uuid4().hex[:6]}"
        
        # Register cleanup on exit
        atexit.register(self.shutdown)

        if not self.docker_image:
            raise ValueError("docker_image must be provided to DDClusterManager")

        # Ensure network exists
        try:
            self.docker_client.networks.get(self.network_name)
        except docker.errors.NotFound:
            self.docker_client.networks.create(self.network_name, driver="bridge")

    @property
    def get_dask_client(self) -> Client:
        return self.dask_client

    def _default_memory_limit(self, n_workers: int) -> str:
        # Give Dask ~85% of the total system memory visible to python
        total_mb = (psutil.virtual_memory().total / (1024 ** 2)) * 0.85
        per_worker_mb = max(256, int(total_mb // max(1, n_workers)))
        return f"{per_worker_mb}MB"
    
    def _find_free_port(self) -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0))
            return s.getsockname()[1]

    def _launch_scheduler(self, ports: Dict[str, int] | None = None, working_dir: Optional[str] = None) -> None:
        ports = ports or {'8786/tcp': 8786, '8787/tcp': 8787}
        name = f"{self._container_prefix}-scheduler"
        try:
            self.scheduler = self.docker_client.containers.run(
                self.docker_image,
                command="dask-scheduler --host 0.0.0.0 --dashboard-address :8787",
                name=name,
                network=self.network_name,
                detach=True,
                ports=ports,
                working_dir=working_dir,
            )
            print(f"Dask Scheduler started: {name} ({self.scheduler.short_id})")
        except docker.errors.APIError as e:
             raise RuntimeError(f"Failed to start scheduler: {e}")

    def _launch_worker(self, idx: int, n_threads: int, memory_limit: str, volumes: Dict[str, dict] | None, env_vars: Dict[str, str] | None = None, working_dir: Optional[str] = None) -> None:
        name = f"{self._container_prefix}-worker-{idx}"
        
        # Find a free port on the host to map to the worker
        host_port = self._find_free_port()
        container_port = 8788  # constant inside container, mapped to unique host port
        
        # We need to tell the worker to listen on 0.0.0.0 (inside container)
        # ANd tell the scheduler (and client) to contact it via host.docker.internal:host_port
        # Note: host.docker.internal works on Windows/Mac. On Linux this might need --add-host.
        
        command = (
            f"dask-worker tcp://{self._container_prefix}-scheduler:8786 "
            f"--nthreads {n_threads} "
            f"--memory-limit {memory_limit} "
            f"--listen-address tcp://0.0.0.0:{container_port} "
            f"--contact-address tcp://host.docker.internal:{host_port}"
        )

        try:
            worker = self.docker_client.containers.run(
                self.docker_image,
                command=command,
                name=name,
                network=self.network_name,
                detach=True,
                mem_limit=memory_limit,
                volumes=volumes or {},
                ports={f'{container_port}/tcp': host_port},
                environment=env_vars or {},
                working_dir=working_dir
            )
            self.workers.append(worker)
            print(f"Dask Worker {idx} started: {name} ({worker.short_id}) mapped to port {host_port}")
        except docker.errors.APIError as e:
            raise RuntimeError(f"Failed to start worker {idx}: {e}")

    def create_cluster(self, mode: str = "full", **kwargs: Any) -> None:
        """
        Create a Docker-backed Dask cluster.

        kwargs:
          - n_workers / num_workers (int)
          - threads_per_worker / n_threads (int)
          - memory_limit (str)
          - volumes (dict)
          - ports (dict)
        """
        # print("APPLES") 
        cpu_cores = multiprocessing.cpu_count()

        # harmonize kwargs
        n_workers = kwargs.get("n_workers", kwargs.get("num_workers"))
        threads_per_worker = kwargs.get("threads_per_worker", kwargs.get("n_threads", 1))
        memory_limit = kwargs.get("memory_limit")
        volumes = kwargs.get("volumes", {})
        ports = kwargs.get("ports", None)
        env_vars = kwargs.get("env_vars", {})
        working_dir = kwargs.get("working_dir", None)

        if mode == "full":
            n_workers = n_workers or cpu_cores
            threads_per_worker = threads_per_worker or 1
            memory_limit = memory_limit or self._default_memory_limit(n_workers)
        elif mode == "test":
            n_workers = 1
            threads_per_worker = 1
            total_gb = int(psutil.virtual_memory().total / (1024 ** 3))
            memory_limit = f"{max(1, total_gb // 2)}GB"
            pass 
        elif mode == "custom":
            n_workers = n_workers or cpu_cores
            threads_per_worker = threads_per_worker or 1
            memory_limit = memory_limit or self._default_memory_limit(n_workers)
        else:
            raise ValueError("Invalid mode. Choose 'full', 'test', or 'custom'.")

        # Launch scheduler and workers
        self._launch_scheduler(ports=ports, working_dir=working_dir)
        for i in range(1, n_workers + 1):
            self._launch_worker(i, threads_per_worker, memory_limit, volumes, env_vars, working_dir)
        
        self.dask_client = self._connect_client_with_retry()

        # Print the dashboard address
        dashboard_address = self.dask_client.dashboard_link
        print(f"Dask dashboard is available at: {dashboard_address}")

    def _connect_client_with_retry(
        self,
        attempts: int = 40,
        delay: float = 0.25,
    ) -> Client:
        """
        Connect to the Dask scheduler using the *actual* published host port,
        retrying until the scheduler is ready.
        """
        # Make sure we have the latest container info
        if not self.scheduler:
             raise RuntimeError("Scheduler not started")
             
        self.scheduler.reload()

        port_info = self.scheduler.attrs["NetworkSettings"]["Ports"].get("8786/tcp")
        if not port_info:
            raise RuntimeError("Scheduler port 8786 is not published")

        host_port = port_info[0]["HostPort"]
        address = f"tcp://127.0.0.1:{host_port}"
        
        # Give the scheduler a moment to actually start listening
        time.sleep(1.0)

        last_err = None
        for i in range(attempts):
            try:
                return Client(address, timeout="10s")
            except Exception as e:
                last_err = e
                # Log occasional failures to help debugging
                if i > 0 and i % 5 == 0:
                    print(f"[DDClusterManager] Connection attempt {i+1}/{attempts} failed: {e}")
                time.sleep(delay)

        # If we get here, connection never succeeded
        logs = "<could not retrieve logs>"
        try:
            logs = self.scheduler.logs(tail=200).decode("utf-8", errors="replace")
        except Exception:
            pass
            
        raise RuntimeError(
            f"Could not connect to Dask scheduler at {address}\n"
            f"Last error: {last_err}\n\n"
            f"Scheduler logs:\n{logs}"
        )

    def shutdown(self):
        """Stop and remove all cluster containers."""
        if self.dask_client:
            try:
                self.dask_client.close()
            except Exception:
                pass
            
        if self.scheduler:
            try:
                # print(f"Stopping scheduler {self.scheduler.name}...")
                self.scheduler.stop()
                self.scheduler.remove()
            except Exception as e:
                pass
                # print(f"Error stopping scheduler: {e}")
            self.scheduler = None

        for w in self.workers:
            try:
                # print(f"Stopping worker {w.name}...")
                w.stop()
                w.remove()
            except Exception as e:
                pass
                # print(f"Error stopping worker: {e}")
        self.workers = []
        
    def __del__(self):
        self.shutdown()

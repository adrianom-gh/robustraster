import warnings

def trigger_warning():
    warnings.warn("Sending large graph of size 30.35 MiB.", UserWarning)
    print("This should not print if the warning is raised as an error.")

class GraphTooLargeError(Exception):
    def __init__(self, size_str):
        self.size_str = size_str

def main():
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("error", message=".*Sending large graph.*", category=UserWarning)
            try:
                # Normal execution logic here
                trigger_warning()
            except UserWarning as e:
                if "Sending large graph" in str(e):
                    raise GraphTooLargeError(str(e))
                else:
                    raise
    except GraphTooLargeError as ge:
        print(f"Intercepted GraphTooLargeError! Message: {ge.size_str}")

if __name__ == "__main__":
    main()

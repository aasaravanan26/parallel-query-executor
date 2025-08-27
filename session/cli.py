import logging

def handle_session_command(cmd):
    cmd = cmd.strip().upper()
    if cmd.startswith("SET TRACE LEVEL"):
        parts = cmd.split()
        if len(parts) == 4:
            level = parts[-1]
            if level == "DEBUG":
                logging.getLogger().setLevel(logging.DEBUG)
            elif level == "INFO":
                logging.getLogger().setLevel(logging.INFO)
            elif level in ("WARN", "WARNING"):
                logging.getLogger().setLevel(logging.WARNING)
            elif level == "ERROR":
                logging.getLogger().setLevel(logging.ERROR)
            else:
                ValueError("Invalid session setting")
        else:
            ValueError("Invalid session setting")
        return True
    elif cmd == "SET TRACE OFF":
        logging.getLogger().setLevel(logging.CRITICAL + 1)
        print("LOGGER disabled.")
        return True
    return False
    pass
import logging
from semantic.validator import load_table_schema
from cache.results_cache import clear_all_cache

def handle_session_command(cmd):
    if not cmd:
        return False
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
        print("✅ Trace level set.")
        return True
    elif cmd == "SET TRACE OFF":
        logging.getLogger().setLevel(logging.CRITICAL + 1)
        print("❌ Tracing disabled.")
        return True
    elif cmd == "SET CACHE CLEAR":
        clear_all_cache()
        print("✅ Cache cleared.")
        return True
    return False

def handle_desc_command(cmd, data_dir):
    if not cmd:
        return False
    cmd = cmd.strip().upper()
    
    if cmd.endswith(";"):
        cmd = cmd[:-1]
    
    if cmd.startswith("DESC"):
        table = cmd.split()
        if len(table) == 1:
            ValueError("Specify table to describe")
        elif len(table) > 2:
            ValueError("Too many tables specified")
        else:
            table = cmd.split()[-1]
            _, schema = load_table_schema(data_dir=data_dir, table=table)
            # Get column types
            for name in schema.names:
                field = schema.field(name)
                print(f"{name}: {field.type}")
            return True
    return False
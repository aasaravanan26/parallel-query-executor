class LogicalPlan:
    def __init__(self, col_proj=None, source_tables=None, filter=None, order_by=None, order_dir=None):
        self.col_proj = col_proj
        self.source_tables = source_tables
        self.filter = filter
        self.order_by = order_by
        self.order_dir = order_dir
    
    def __repr__(self):
        return (f"LogicalPlan(\n"
                f"  col_proj={self.col_proj},\n"
                f"  source_tables={self.source_tables},\n"
                f"  filter={self.filter},\n"
                f"  order_by={self.order_by},\n"
                f"  order_dir={self.order_dir}\n"
                f")")
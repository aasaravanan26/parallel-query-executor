class LogicalPlan:
    def __init__(self, col_proj=None, source_tables=None, filter=None, order_by=None, order_dir=None, sel_all=None):
        self.col_proj = col_proj # defaultdict(list) column projections for each source table
        self.source_tables = source_tables # list of source tables
        self.filter = filter # where predicate (during PARSE time)
        self.single_filters = None # where predicate (during SEMANTIC time)
        self.join_filters = None # where predicate joining two tables (during SEMANTIC time)
        self.order_by = order_by # order by columns
        self.order_dir = order_dir # order by direction [ASC, DESC]
        self.sel_all = sel_all # '*' present
    
    def __repr__(self):
        return (f"LogicalPlan(\n"
                f"  col_proj={self.col_proj},\n"
                f"  source_tables={self.source_tables},\n"
                f"  filter={self.filter},\n"
                f"  order_by={self.order_by},\n"
                f"  order_dir={self.order_dir}\n"
                f"  select_all = {self.sel_all})")
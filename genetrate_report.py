from productivity_report import ProductivityReport as pr
from idle_busy_report import IdleBusyReport as ID
generate_productivity = pr()
generate_idlebusy = ID()
if __name__ == '__main__':
    # generate_productivity.productivity_report_generate()
    generate_idlebusy.idle_busy_report_generate()

from pandas.tseries.holiday import *
from pandas.tseries.offsets import CustomBusinessDay

class NigeriaCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday('New Year', month=1, day=1, observance=sunday_to_monday),
        Holiday('Good Friday', month=4, day=2, observance=sunday_to_monday),
        Holiday('Easter Monday', month=4, day=5, observance=sunday_to_monday),
        Holiday('Workers Day', month=5, day=1, observance=sunday_to_monday),
        Holiday('Eid-el-fitri Sallah Holiday', month=5, day=12),
        Holiday('Democracy Day', month=6, day=14, observance=sunday_to_monday),
        Holiday('Id el Kabir', month=7, day=20, observance=nearest_workday),
        Holiday('Id el Kabir Holiday', month=7,
                day=21, observance=sunday_to_monday),
        Holiday('Independence Day', month=10,
                day=1, observance=sunday_to_monday),
        Holiday('Eidul-Mawlid', month=10, day=19, observance=nearest_workday),
        Holiday('Christmas Day', month=12, day=25, observance=nearest_workday),
        Holiday('Boxing Day', month=12, day=26),
        Holiday('Christmas Day', month=12, day=27, observance=nearest_workday),
        Holiday('Boxing Day', month=12, day=28)
    ]
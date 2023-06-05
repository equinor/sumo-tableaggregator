from sumo.wrapper import SumoClient
from sumo.table_aggregation.utilities import get_expiry_time, check_or_refresh_token


def test_expiry_time():
    sumo = SumoClient("dev")
    exp = get_expiry_time(sumo)


def test_check_or_refresh():
    sumo = SumoClient("dev")
    check_or_refresh_token(sumo)


if __name__ == "__main__":
    # test_expiry_time()
    test_check_or_refresh()

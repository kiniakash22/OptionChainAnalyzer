import requests
import datetime
import json
import os
from prettytable import PrettyTable
import sys


UPDATE_FREQ = 0.01


class OptionChainAnalyzer:

    script_info = {
        "NIFTY": {
            "oc_url": "https://services.niftytrader.in/options/fetchNseOptionsDataNew?symbol=nifty&strikePrice=0",
            "spot_url": "https://api.niftytrader.in/webapi/Symbol/getSymbolSpotData?symbol=NIFTY%2050",
            "strike_diff": 50
        },
        "BANKNIFTY": {
            "oc_url": "https://services.niftytrader.in/options/fetchNseOptionsDataNew?symbol=banknifty&strikePrice=0",
            "spot_url": "https://api.niftytrader.in/webapi/Symbol/getSymbolSpotData?symbol=NIFTY%20BANK",
            "strike_diff": 100
        }
    }

    def __init__(self, script="NIFTY", strikes=10):
        """
        Initializes all the variables: current_weekly_exp_date, script (default is 'nifty'), oc_url, option_chain_json
        """
        self.current_weekly_exp_date = self.get_current_weekly_exp_date()
        self.script = script
        self.strikes = strikes
        self.oc_url = self.script_info[self.script]['oc_url']
        self.spot_cmp_url = self.script_info[self.script]['spot_url']
        self.strike_diff = self.script_info[self.script]['strike_diff']
        self.history_set = set()
        self.option_chain_json = dict()

        self.spot_cmp = self.get_cmp()
        self.rounded_spot_cmp = round(self.spot_cmp / self.strike_diff) * self.strike_diff

    def print_variables(self):
        """
        Prints all the initialized variables:  current_weekly_exp_date, script, spot_cmp, strikes
        """

        print(f"Current Weekly Expiry: {self.current_weekly_exp_date}")
        print(f"Get OC for Script: {self.script}")
        print(f"CMP: {self.spot_cmp}")
        print(f"Data will be fetched for '{self.strikes}' strikes")

    def get_current_weekly_exp_date(self):
        """
        Gets the current weekly expiry date i.e the coming Thrusday
        """

        date = datetime.date.today()
        while date.weekday() != 3:
            date += datetime.timedelta(1)
        return date.strftime("%Y-%m-%d")

    def get_option_chain(self):
        """
        Retruns a list of json entries applicable only for current expiry
        """
        # self.current_weekly_exp_date = "2022-03-31"
        new_entry = dict()
        self.option_chain_json[self.current_weekly_exp_date] = dict()
        all_option_chain = requests.get(self.oc_url).json()
        for entry in all_option_chain['resultData']:
            entry_strike_price = entry['strike_price']
            if entry['expiry_date'][:10] == self.current_weekly_exp_date and (entry_strike_price in range(self.min_strike, self.max_strike + 1)):
                new_entry = dict()
                new_entry['CE'] = dict()
                new_entry['PE'] = dict()

                new_entry['CE']['oi'] = entry['calls_oi']
                new_entry['CE']['oi_change'] = entry['calls_change_oi']
                new_entry['CE']['vol'] = entry['calls_volume']

                new_entry['PE']['oi'] = entry['puts_oi']
                new_entry['PE']['oi_change'] = entry['puts_change_oi']
                new_entry['PE']['vol'] = entry['puts_volume']

                self.option_chain_json[self.current_weekly_exp_date][entry_strike_price] = new_entry
                # print(f"OC JSON: {option_chain_json}")
                new_entry = dict()
        self.dump_data()
        return self.option_chain_json

    def get_cmp(self):
        """
        Returns current market price for the script
        """
        return requests.get(self.spot_cmp_url).json()['resultData']['nifty_value']

    def dump_data(self):
        folder_name = start_time.strftime("%Y-%m-%d")
        json_file_name = start_time.strftime("%H_%M")+"_data"

        if not os.path.isdir(f"{self.script}_data"):
            os.makedirs(f"{self.script}_data")
        if not os.path.isdir(f"{self.script}_data/{folder_name}"):
            os.makedirs(f"{self.script}_data/{folder_name}")

        if os.path.isfile(f"{self.script}_data/{folder_name}/history.txt"):
            with open(f"{self.script}_data/{folder_name}/history.txt") as f:
                # print("\nReading Data from histiry.txt")
                self.history_set = set(f.read().splitlines())
        self.history_set = set(sorted(self.history_set, reverse=True)[1:])
        self.history_set.add(f"{self.script}_data/{folder_name}/"+json_file_name+".json")

        # print(f"Current 'history_set': {self.history_set}")
        self.history_set.add("_IST "+start_time.strftime('%H:%M:00'))
        self.history_set = sorted(self.history_set, reverse=True)
        history_file = open(f"{self.script}_data/{folder_name}/history.txt", "w")

        with open(f'{self.script}_data/{folder_name}/{json_file_name}.json', 'w') as f:
            json.dump(self.option_chain_json, f, ensure_ascii=False, indent=4)

        for history in self.history_set:
            history_file.write(history+"\n")

        history_file.close()

        pass

    def fetch_option_chain_data(self):
        self.min_strike = self.rounded_spot_cmp - (self.strike_diff * self.strikes)
        self.max_strike = self.rounded_spot_cmp + (self.strike_diff * self.strikes)
        self.option_chain_json = self.get_option_chain()
        print(f"{start_time.strftime('%Y-%m-%d')}: Fetched Option chain for '{self.script}' for Expiry '{self.current_weekly_exp_date}' and the Strike Range: {self.min_strike} - {self.max_strike}")
        self.get_last_update()

    def get_last_update(self):
        folder_name = start_time.strftime("%Y-%m-%d")
        if os.path.isfile(f"{self.script}_data/{folder_name}/history.txt"):
            with open(f"{self.script}_data/{folder_name}/history.txt") as f:
                self.history_set = set(f.read().splitlines())
        self.history_set = sorted(self.history_set, reverse=True)
        # self.last_update_time = self.get_history(self.script)
        return self.history_set[:3]

    def read_from_json(self, filename):
        with open(filename) as json_file:
            data = json.load(json_file)
        return data

    def analyze_option_chain_data(self):
        self.last_update = self.get_last_update()
        self.time_since_last_update = 9999
        if len(self.last_update) == 3:
            self.last_update_time, self.last_two_files = self.last_update[0].split(" ")[1], self.last_update[1:]
            # print(f"Last update time: {self.last_update_time}")
            self.time_since_last_update = (datetime.datetime.strptime(start_time.strftime("%H:%M:%S"), "%H:%M:%S") - datetime.datetime.strptime(self.last_update_time, "%H:%M:%S")).total_seconds() / 60
        if self.time_since_last_update > UPDATE_FREQ or len(self.last_update) != 3:
            # print("Need to update")
            self.fetch_option_chain_data()
            self.last_update = self.get_last_update()
            self.last_update_time, self.last_two_files = self.last_update[0].split(" ")[1], self.last_update[1:]
            # print(f"Last update time: {self.last_update_time}")

        if len(self.last_update) == 3:
            # print(f"Current data: {self.last_two_files[0]}")
            # print(f"Previous data: {self.last_two_files[1]}")
            self.current_data = self.read_from_json(self.last_two_files[0])
            self.previous_data = self.read_from_json(self.last_two_files[1])
            self.compare_latest_oc_data_with_prev()

    def compare_latest_oc_data_with_prev(self):
        pt = PrettyTable()
        h, m, _ = self.last_two_files[1].split("/")[-1].split("_")
        prev_time = f"{h}:{m}"
        h, m, _ = self.last_two_files[0].split("/")[-1].split("_")
        curr_time = f"{h}:{m}"
        pt.field_names = [f"CE OI PREV ({prev_time})", f"CE OI ({curr_time})", "CE OI CHANGE", "CE OI CHANGE (%)", "STRIKE", "PE OI CHANGE (%)", "PE OI CHANGE", f"PE OI ({curr_time})", f"PE OI PREV ({prev_time})"]
        for strike, strike_data in self.current_data[self.current_weekly_exp_date].items():
            if strike not in self.previous_data[self.current_weekly_exp_date].keys():
                continue
            CE_OI_CHANGE_PREV = strike_data['CE']['oi'] - self.previous_data[self.current_weekly_exp_date][strike]['CE']['oi']
            CE_OI_CHANGE_PREV_PER = round(((strike_data['CE']['oi']/self.previous_data[self.current_weekly_exp_date][strike]['CE']['oi']) * 100) - 100, 2)
            PE_OI_CHANGE_PREV = strike_data['PE']['oi'] - self.previous_data[self.current_weekly_exp_date][strike]['PE']['oi']
            PE_OI_CHANGE_PREV_PER = round(((strike_data['PE']['oi']/self.previous_data[self.current_weekly_exp_date][strike]['PE']['oi']) * 100) - 100, 2)
            strike_data['CE']['oi_change_with_previous'] = CE_OI_CHANGE_PREV
            strike_data['PE']['oi_change_with_previous'] = PE_OI_CHANGE_PREV
            CE_OI = strike_data['CE']['oi']
            CE_OI_PREV = self.previous_data[self.current_weekly_exp_date][strike]['CE']['oi']
            PE_OI = strike_data['PE']['oi']
            PE_OI_CHANGE = self.previous_data[self.current_weekly_exp_date][strike]['PE']['oi']
            if strike == str(self.rounded_spot_cmp):
                strike = "> " + strike + " <"
            pt.add_row([CE_OI_PREV/1000, CE_OI/1000,  CE_OI_CHANGE_PREV/1000,  f"{CE_OI_CHANGE_PREV_PER:+} %", strike, f"{PE_OI_CHANGE_PREV_PER:+} %",  PE_OI_CHANGE_PREV/1000,  PE_OI/1000,  PE_OI_CHANGE/1000])
        print(pt)


start_time = datetime.datetime.now()

print("="*150)
print(f"Start: {start_time}")

if (len(sys.argv) == 3):
    script = sys.argv[1]
    strikes = int(sys.argv[2])
else:
    print("Not all required parameter are passed. Using default values.")
    script = "NIFTY"
    strikes = 10

print(f"Script: {script} and Strikes: {strikes}")
oca_nifty = OptionChainAnalyzer(script, strikes)
oca_nifty.analyze_option_chain_data()

print(f"Script took {round((datetime.datetime.now() - start_time).total_seconds() * 1000, 2)} ms")

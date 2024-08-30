from dune_client.client import DuneClient

from utils.Settings import Setting
setting = Setting()

if __name__ == '__main__':
    dune = DuneClient(setting.DUNE_API_KEY)
    query_result = dune.get_latest_result_dataframe(3237025)
    query_result.to_csv("cex_address.csv", index=False)
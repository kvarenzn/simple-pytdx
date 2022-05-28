from api import Api

if __name__ == '__main__':
    # 使用上证云成都电信一
    with Api(('218.6.170.47', 7709)) as api:
        print(api.get_stocks_list(Api.Market.SH, 0))
        print(len(api.get_stocks_list(Api.Market.SH, 0)))
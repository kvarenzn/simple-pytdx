from api import Api
from pprint import pprint

if __name__ == '__main__':
    # 使用上证云成都电信一
    with Api(('218.6.170.47', 7709)) as api:
        # 南大光电
        for data in api.get_xdxr_info(api.Market.SZ, '000778'):
            print(data['日期'], data['类型'], end=' ')
            del data['日期']
            del data['类型']
            for k, v in data.items():
                print(f'{k}: {v}, ', end='')
            print()

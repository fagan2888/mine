import os
import pandas as pd

if __name__ == '__main__':
    before_dir = '/opt/data/user/jzhang/country_cover/alphaBefore'
    after_dir = '/opt/data/user/jzhang/country_cover/alphaAfter'
    corr_dir = '/opt/data/user/jzhang/country_cover/corr/'
    univs = ['npxchnpak','npem']
    # univs = ['npxchnpak']
    _d = '20191013'

    for u in univs:
        print(u)
        before_dir_cur = before_dir + '/' + u
        after_dir_cur = after_dir + '/' + u
        for dI in os.listdir(before_dir_cur):
            if dI != '.' and dI != '..':
                before = pd.read_csv(before_dir_cur + '/' + dI + '/' + dI + '_' + _d + '.alp', header=None)
                after = pd.read_csv(after_dir_cur + '/' + dI + '/' + dI + '_' + _d + '.alp', header=None)

                cr = before.merge(after, how='outer', on=['X.1'])

                cr2 = cr[['X.2.x','X.2.y']].corr()
                print(dI+':')
                print(cr2)

                cr2.to_csv(corr_dir+dI+'.csv')

            # print(dI)



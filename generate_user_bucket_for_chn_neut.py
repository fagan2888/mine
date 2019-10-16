from nipun.dataproviders.benchmark_data_provider import BenchmarkDataProvider
from nipun.utils import write_alpha_files
import datetime
import pandas as pd
import nipun.dataproviders.universe_provider as up
# pandas.__version__

ALPHADIR = '/opt/data/user/jzhang/neut/'
SIG_NAME = 'msci_chinaintl'

if __name__ == '__main__':
    start_date = datetime.datetime(2017, 10, 1)
    end_date = datetime.datetime.now()
    dates = pd.DateRange(start_date, end_date, offset=pd.datetools.MonthEnd())
    IN_INDEX = 'IN_IDX'
    universeProvider = up.UniverseProvider('npchn', daily=True)
    for _d in dates:
        _bm = BenchmarkDataProvider(_d - pd.datetools.BDay(), code='msci_chinaintl', proforma=False,
                                    look_back_days=0).universe
        _bm = pd.DataFrame(index=_bm, columns=[IN_INDEX], data=['YES' for i in _bm])
        universe = universeProvider.get_data(_d)
        _bm = _bm.reindex(index=universe.index).fillna('NO')
        write_alpha_files(_bm, SIG_NAME, _d, production=False, alpdir=ALPHADIR)
        # x=1
        # pass
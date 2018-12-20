import pandas as pd

iperf_timestamp_format = '%Y%m%d%H%M%S.%f'

def parse(filepath_or_buffer):
    """takes an iperf csv (run iperf with --reportstyle C) and returns a pandas dataframe that filters out the aggregate rows
    Parameters
    ----------
    filepath_or_buffer : str, pathlib.Path, py._path.local.LocalPath or any object with a read() method (such as a file handle or StringIO)
        
    Returns
    -------
    result : pandas.DataFrame"""
    
    iperf_names = 'timestamp,source_address,source_port,destination_address,destination_port,flow_id,interval,transferred_bytes,bits_per_second'.split(',')

    df = pd.read_csv(filepath_or_buffer,
                     names=iperf_names,
                     header=None,
                     parse_dates=['timestamp'],
                     date_parser=lambda x: pd.datetime.strptime(x, iperf_timestamp_format))
    df["mbps"] = df["bits_per_second"]*1e-6
    starts,ends = zip(*[[float(x) for x in row.interval.split('-')] for _,row in df.iterrows() ])
    df['start'],df['end'] = starts,ends
    df = df[df.flow_id >= 0]
    # filter out the aggregate row
    df = df[df.apply(lambda r: not(r.start == min(starts) and r.end == max(ends)), axis=1)]
    return df

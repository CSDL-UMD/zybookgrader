""" ZyBook grading script """

import os
import datetime
import dateutil.tz
import sys
import re
import argparse
import pandas

# columns
KEY_COLS = [
    'last_name', 
    'first_name', 
    'primary_email', 
    'school_email', 
    'student_id'
]

# patterns
PAT_TS = '(?P<timestamp>\d\d\d\d-\d\d-\d\d_\d\d\d\d)(_(?P<tz>[^.]*))?'
PAT_TS_STRPTIME = '%Y-%m-%d_%H%M'
PAT_POINTS = '\((?P<pts>\d+)\)$'

# default file names
OUTPUT_GRADES = 'grades.csv'
OUTPUT_GRADES_FULL = 'grades_by_day.csv'


def find(seq, pattern, matcher=re.match):
    """
    Search pattern in each element in sequence and return the first
    element that matches the pattern. (Like re.search but for lists.)
    """
    for elem in seq:
        if matcher(pattern, elem) is not None:
            return elem


def droptotals(df):
    """
    drop all "total" columns
    """
    idx = df.columns.str.contains("total")
    df = df.copy()
    return df.loc[:, ~idx]


def dropmisc(df):
    """
    drop additional misc columns:
        * points_earned_(out_of_XX)
        * percent_grade
    """
    to_drop = []
    # drop "points_earned_(out_of_XX)"
    col = find(df.columns, '^points_earned')
    to_drop.append(col)
    # drop "percent_grade"
    col = find(df.columns, '^percent_grade')
    to_drop.append(col)
    df = df.copy()
    return df.drop(to_drop, axis=1, errors="ignore")


def readassignment(path):
    """
    Read data frame with assignment report.
    """
    df = (pandas.read_csv(path)
          .rename(columns=str.lower)
          .rename(columns=lambda k: k.replace(" ", "_"))
          .assign(due_date=lambda k: k['due_date'].apply(pandas.Timestamp))
          .assign(due_date=lambda k: k['due_date'].dt.tz_convert("UTC"))
          .pipe(droptotals)
          .pipe(dropmisc)
          .pipe(topoints)
          .pipe(fillnasafe)
          )
    return df


def matchpointstotal(s):
    """ Return X (as an int) from string like "total_(X)" """
    m = re.search(PAT_POINTS, s)
    pts = m.groupdict()['pts']
    pts = int(pts)
    return pts


def _topoints(s):
    """
    Convert series with percentage grades back to points
    """
    pts = matchpointstotal(s.name)
    return (pts * s / 100).round()


def topoints(df):
    """
    Convert all columns with percentages in data frame back to points
    """
    df = df.copy()
    for col in df.columns:
        if re.search(PAT_POINTS, col):
            df[col] = _topoints(df[col])
    return df


def fillnasafe(df):
    """
    Fill missing values making sure not to mix strings and numbers 
    """
    fv = dict.fromkeys(df.columns, 0)
    for col in KEY_COLS:
        fv[col] = ''
    df = df.copy()
    df = df.fillna(fv)
    return df


def matchdatefromfilename(s):
    """
    Extract the date of the report file from zybooks from its filename
    """
    m = re.search(PAT_TS, s)
    if m is None:
        print("Error: not a valid grade file: {}".format(s),
                file=sys.stderr)
        sys.exit(1)
    groups = m.groupdict()
    ts = groups['timestamp']
    ts = datetime.datetime.strptime(ts, PAT_TS_STRPTIME)
    if 'tz' in groups and groups['tz'] is not None:
        tz = dateutil.tz.gettz(groups['tz'])
        ts = ts.replace(tzinfo=tz)
        ts = pandas.Timestamp(ts).astimezone("UTC")
    else:
        ts = pandas.Timestamp(ts)
    return ts


def readonereport(fp):
    """
    Read data frame with points report. Add report date as a column.
    """
    report_date = matchdatefromfilename(getattr(fp, 'name', fp))
    df = (pandas.read_csv(fp)
          .rename(columns=str.lower)
          .rename(columns=lambda k: k.replace(" ", "_"))
          .pipe(droptotals)
          .pipe(topoints)
          .pipe(fillnasafe)
          .assign(day=report_date)
          )
    return df


def readmanyreports(*fp_seq):
    """
    Read and combine all report data into a single data frame.
    """
    tmp = []
    for fp in fp_seq:
        df = readonereport(fp)
        tmp.append(df)
    df = (pandas.concat(tmp)
          .set_index('day')
          .sort_index(axis=0)
          .reset_index())
    return df
        

def dropwithsuffix(df, suffix):
    """
    Drop all columns whose name ends with given suffix
    """
    idx = df.columns.str.endswith(suffix)
    df = df.copy()
    return df.loc[:, ~idx]


def dropextrapoints(df, df_hw):
    """
    Drop points columns that are not in assignment
    """
    pts_col_df = df.filter(regex=PAT_POINTS, axis=1).columns
    pts_col_hw = df_hw.filter(regex=PAT_POINTS, axis=1).columns
    col_to_keep = pts_col_df.intersection(pts_col_hw)
    col_to_drop = pts_col_df.difference(col_to_keep)
    df = df.copy()
    df = df.drop(col_to_drop, axis=1, errors='ignore')
    return df



def read(*fp_seq, assignment_fp=None):
    """
    Read all report files downloaded from zybooks. If the path to an assignment
    file is specified, read it and compute points earned each late day.
    """
    DROP_SUFFIX = '_drop'
    df = readmanyreports(*fp_seq)
    if assignment_fp is not None:
        # read assignment
        df_hw = readassignment(assignment_fp)
        # drop extra columns not in assignment
        df = dropextrapoints(df, df_hw)
        # join and drop duplicates (marked by suffix)
        df = (df_hw
            .set_index(KEY_COLS)
            .join(df.set_index(KEY_COLS), rsuffix=DROP_SUFFIX)
            .pipe(dropwithsuffix, DROP_SUFFIX)
            .sort_index())
        # compute point increments and late days
        df = (df
              .groupby(by=df.index.names)
              .diff()
              .fillna(df)
              .assign(due_date=df['due_date'])
              .assign(day=df['day'])
              .assign(days_late=(df['day'] - df['due_date']).astype('m8[D]'))
              .reset_index())
    else:
        # compute point increments only
        df = df.set_index(KEY_COLS).sort_index()
        df = (df
              .groupby(by=df.index.names)
              .diff()
              .fillna(df)
              .assign(day=df['day'])
              .reset_index())
    # compute point total
    pts_col = df.filter(regex=PAT_POINTS, axis=1).columns
    pts_tot = int(df.columns.str.extractall(PAT_POINTS).astype(int).sum())
    name = 'total_({:d})'.format(pts_tot)
    df[name] = df[pts_col].sum(axis=1)
    return df


def deductpoints(df, penalty):
    """
    Deduct points earned on late days
    """
    df = df.copy()
    pen = penalty / 100
    for col in df.columns:
        if re.search(PAT_POINTS, col):
            df[col] -= df['days_late'].clip(0) * pen * df[col]
    return df


def finalgrade(df, threshold):
    """
    Compute final grade based on threshold.
    """
    df = df.copy()
    total_col = find(df.columns, 'total_')
    total_pts = matchpointstotal(total_col)
    df = (df
          .drop(["due_date", "day", "days_late"], axis=1, errors='ignore')
          .groupby(KEY_COLS)
          .sum()
          .reset_index())
    df['total'] = df[total_col] / total_pts * 100
    df['final'] = df.apply(scorefun, axis=1, args=(threshold,))
    df['final_pts'] = df['final'] / 100 * total_pts
    return df


def scorefun(x, threshold=70):
    """
    Score function used by `finalgrade`
    """
    if x['total'] >= threshold:
        return 100
    else:
        return x['total']


def summarize(df):
    """
    Compute summary of points earned by day
    """
    total_col = find(df.columns, 'total_')
    df_summary = pandas.crosstab(
        index=[df[k] for k in KEY_COLS],
        columns=df['days_late'],
        values=df[total_col],
        aggfunc='sum',
        margins=True).reset_index().fillna(0)
    return df_summary


def makeparser():
    """
    Create parser for command-line arguments
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports_fp", 
                        type=argparse.FileType('r'),
                        nargs="+", 
                        metavar="PATH",
                        help="Report file(s) from zyBook with points")
    parser.add_argument("-D",
                        "--due-dates",
                        dest="assignment_fp",
                        metavar="PATH",
                        type=argparse.FileType('r'),
                        help="Report file from zyBook with assignment due dates")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-F", 
                        "--full-grade-at", 
                        type=int,
                        default=70,
                        metavar='PCT',
                        dest="threshold",
                        help="Give full grade above this %% (default: %(default)d%%)")
    group.add_argument("-N",
                        "--no-threshold",
                        action='store_const',
                        const=100,
                        dest='threshold',
                        help="Do not apply threshold")
    parser.add_argument("-P", 
                        "--penalty-factor", 
                        type=int,
                        default=20,
                        metavar='PCT',
                        dest="penalty",
                        help="Deduct points earned late (default: -%(default)d%%/day) ")
    parser.add_argument("-o", 
                        "--output",
                        type=argparse.FileType('w'),
                        metavar="PATH",
                        default=OUTPUT_GRADES,
                        help="Write results to path (default: %(default)s)")
    parser.add_argument("-O",
                        "--output-summary",
                        metavar="PATH",
                        type=argparse.FileType('w'),
                        default=OUTPUT_GRADES_FULL,
                        help="Write daily point summary to path"
                        " (default: %(default)s")
    return parser


def _main(args, parser):
    """
    Main function called by module entry point. This function returns.
    """
    if len(args.reports_fp) > 1:
        print("Reading points from:")
        for fp in args.reports_fp:
            print(" - {}".format(fp.name))
    else:
        print("Reading points from: {}".format(args.reports_fp[0].name))
    if args.assignment_fp is not None:
        print("Reading due dates from: {}".format(args.assignment_fp.name))
    df = read(*args.reports_fp, assignment_fp=args.assignment_fp)
    if len(args.reports_fp) > 1:
        print("Computing total points by day...")
        df_2 = summarize(df)
        df_2.to_csv(args.output_summary, index=False)
        print("Written: {}".format(args.output_summary.name))
    else:
        # clean up
        args.output_summary.close()
        os.delete(args.output_summary)
    if args.assignment_fp is not None:
        print("Applying -{}%/day penalty...".format(args.penalty)) 
        df = deductpoints(df, args.penalty)
    if args.threshold < 100:
        print("Setting full grades at {}%.".format(args.threshold))
    else:
        print("No grade threshold applied.")
    df = finalgrade(df, args.threshold)
    df.to_csv(args.output, index=False)
    print("Written: {}".format(args.output.name))
    return df


def main():
    """
    Setuptools entry point for this module. Running the module as a
    script calls this function. This function does not return.
    """
    parser = makeparser()
    args = parser.parse_args()
    _main(args, parser)


if __name__ == '__main__':
    main()

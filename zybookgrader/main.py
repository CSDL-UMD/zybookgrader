import os
import datetime
import sys
import re
import argparse
import pandas


PAT_HW = "Week_\d\d_Homework"

HW_COLS = ['last_name', 'first_name', 'primary_email', 'school_email',
                  'student_id', 'due_date']

PAT_TS = '\d\d\d\d-\d\d-\d\d_\d\d\d\d'
PAT_TS_STRPTIME = '%Y-%m-%d_%H%M'
PAT_POINTS = '\((?P<pts>\d+)\)'

KEY_COLS = ['last_name', 'first_name', 'primary_email', 'school_email', 'student_id']
DAY_COL = 'day'

OUTPUT_GRADES = 'grades.csv'
OUTPUT_GRADES_FULL = 'grades_by_day.csv'


def find(seq, pattern, matcher=re.match):
    """
    Search pattern in each element in sequence and return the first element
    that matches
    """
    for elem in seq:
        if matcher(pattern, elem) is not None:
            return elem


def read_assignment_frame(path):
    df = (pandas.read_csv(path)
          .rename(columns=str.lower)
          .rename(columns=lambda k: k.replace(" ", "_"))
          .assign(due_date=lambda k: k['due_date'].apply(pandas.Timestamp))
          )
    fv = dict.fromkeys(df.columns, 0)
    for col in KEY_COLS:
        fv[col] = ''
    df = df.fillna(fv)
    return df[HW_COLS]


def findpointstotal(name):
    m = re.search(PAT_POINTS, name)
    pts = m.groupdict()['pts']
    pts = int(pts)
    return pts


def to_points(s):
    total = findpointstotal(s.name)
    return (total * s / 100).round()


def read_report_frame(fp):
    df = (pandas.read_csv(fp)
          .rename(columns=str.lower)
          .rename(columns=lambda k: k.replace(" ", "_"))
          )
    for col in df.columns:
        if re.search(PAT_POINTS, col):
            df[col] = to_points(df[col])
    fv = dict.fromkeys(df.columns, 0)
    for col in KEY_COLS:
        fv[col] = ''
    df = df.fillna(fv)
    return df


def read_reports_frames(*fp_seq):
    """
    Arguments
    =========
    fp_seq - list of FileType objects
    """
    tmp = []
    for fp in fp_seq:
        m = re.search(PAT_TS, fp.name)
        if m is None:
            print("Error: not a valid grade file: {}".format(p.name),
                  file=sys.stderr)
            print("  please specify pattern (-p/--pattern)", 
                  file=sys.stderr)
            sys.exit(1)
        ts = m.group()
        ts = datetime.datetime.strptime(ts, PAT_TS_STRPTIME)
        ts = pandas.Timestamp(ts)
        df = read_report_frame(fp)
        df[DAY_COL] = ts
        tmp.append(df)
    df = (pandas.concat(tmp)
          .set_index(DAY_COL)
          .sort_index(axis=0)
          .reset_index())
    return df
        

def read_frames(*fp_seq, assignment_fp=None):
    df = read_reports_frames(*fp_seq)
    if assignment_fp is not None:
        # join with due date info
        df_hw = read_assignment_frame(assignment_fp)
        df = (df_hw
            .set_index(KEY_COLS)
            .join(df.set_index(KEY_COLS))
            .sort_index())
        df_late = (df.groupby(by=df.index.names).diff()
                   .fillna(df)
                   .assign(due_date=df['due_date'])
                   .assign(day=df['day'])
                   .assign(days_late=(df['day'] - df['due_date']).astype('m8[D]'))
                   .reset_index())
        return df_late
    else:
        # do not join
        return df


def apply_penalty(df, penalty):
    pen = penalty / 100
    for col in df.columns:
        if re.search(PAT_POINTS, col):
            df[col] -= df['days_late'].clip(0) * pen * df[col]
    return df


def set_full_grade(df, threshold):
    total_col = find(df.columns, 'total_')
    total_pts = findpointstotal(total_col)
    df = (df
          .drop(["due_date", "day", "days_late"], axis=1, errors='ignore')
          .groupby(KEY_COLS)
          .sum()
          .reset_index())
    df['total'] = df[total_col] / total_pts * 100
    df['final'] = df.apply(final_score, axis=1, args=(threshold,))
    df['final_pts'] = df['final'] / 100 * total_pts
    return df


def final_score(x, threshold=70):
    if x['total'] >= threshold:
        return 100
    else:
        return x['total']


def make_parser():
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
    parser.add_argument("-F", 
                        "--full-grade-at", 
                        type=int,
                        default=70,
                        metavar='PCT',
                        dest="threshold",
                        help="Give full grade above this %% (default: %(default)d%%)")
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
    return parser


# Main entry point
def main():
    parser = make_parser()
    args = parser.parse_args()
    if len(args.reports_fp) > 1:
        print("Reading points from:")
        for fp in args.reports_fp:
            print(" - {}".format(fp.name))
    else:
        print("Reading points from: {}".format(args.reports_fp[0].name))
    df = read_frames(*args.reports_fp, assignment_fp=args.assignment_fp)
    # df.to_csv("grades_by_day.csv", index=False)
    # print("Written: grades_by_day.csv")
    if args.assignment_fp is not None:
        print("Reading due dates from: {}".format(args.assignment_fp.name))
        print("Applying -{}%/day penalty...".format(args.penalty)) 
        df = apply_penalty(df, args.penalty)
    print("Setting full grades at {}%".format(args.threshold))
    df = set_full_grade(df, args.threshold)
    df.to_csv(args.output, index=False)
    print("Written: {}".format(args.output.name))


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
import argparse, time, os, csv, random
parser = argparse.ArgumentParser()
parser.add_argument("input")
parser.add_argument("--out", required=True)
parser.add_argument("--cut", type=int, default=0)
parser.add_argument("--max-events", type=int, default=1000)
args = parser.parse_args()
os.makedirs(args.out, exist_ok=True)
outcsv = os.path.join(args.out, "toy_out.csv")
# pretend to process events
time.sleep(random.uniform(0.5, 1.5))
with open(outcsv, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["event_id", "value"])
    for i in range(min(100, args.max_events)):
        writer.writerow([i, random.random() * args.cut])
print("Wrote", outcsv)

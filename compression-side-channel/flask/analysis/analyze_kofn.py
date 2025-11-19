#!/usr/bin/env python3
"""
analyze_kofn.py - Analyze K-of-N Attack Results
Generate paper-style tables from evaluation CSV files

Usage:
  python analyze_kofn.py --input_dir ./logs --output_dir ./results

Output:
  - Summary tables (accuracy vs queries by configuration)
  - Comparison between baseline and G-DBREACH modes
  - Statistical analysis (mean, std, confidence intervals)
"""
import os
import sys
import csv
import glob
import argparse
from collections import defaultdict
import statistics

def parse_args():
    parser = argparse.ArgumentParser(description="Analyze K-of-N attack results")
    parser.add_argument("--input_dir", default="./logs",
                        help="Directory containing result CSVs")
    parser.add_argument("--output_dir", default="./results",
                        help="Directory for output tables")
    parser.add_argument("--format", default="markdown", choices=["markdown", "csv", "latex"],
                        help="Output format")
    return parser.parse_args()

def load_all_results(input_dir):
    """Load all CSV files from input directory"""
    results = []
    csv_files = glob.glob(os.path.join(input_dir, "*.csv"))

    for csv_file in csv_files:
        try:
            with open(csv_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Convert numeric fields
                    for key in ['n', 'k', 'trial_id', 'tp', 'fp', 'fn', 'tn', 'db_queries', 'setup_attempts']:
                        if key in row and row[key]:
                            try:
                                row[key] = int(row[key])
                            except:
                                pass

                    for key in ['topk_exact_match', 'topk_partial_accuracy', 'per_string_accuracy',
                               'precision', 'recall', 'f1', 'setup_time', 'attack_time', 'total_time']:
                        if key in row and row[key]:
                            try:
                                row[key] = float(row[key])
                            except:
                                pass

                    # Filter successful trials
                    if row.get('success') == 'True' or row.get('success') == True:
                        results.append(row)
        except Exception as e:
            print(f"[WARN] Failed to load {csv_file}: {e}")

    return results

def group_results(results):
    """Group results by (engine, dataset, n, k, mode)"""
    grouped = defaultdict(list)

    for r in results:
        key = (
            r.get('engine', 'unknown'),
            r.get('dataset', 'unknown'),
            r.get('n', 0),
            r.get('k', 0),
            r.get('mode', 'unknown')
        )
        grouped[key].append(r)

    return grouped

def compute_stats(values):
    """Compute mean, std, min, max for a list of values"""
    if not values:
        return {'mean': 0, 'std': 0, 'min': 0, 'max': 0, 'count': 0}

    n = len(values)
    mean = sum(values) / n
    std = statistics.stdev(values) if n > 1 else 0

    return {
        'mean': mean,
        'std': std,
        'min': min(values),
        'max': max(values),
        'count': n
    }

def generate_summary_table(grouped_results):
    """Generate summary statistics for each configuration"""
    summary = []

    for key, trials in sorted(grouped_results.items()):
        engine, dataset, n, k, mode = key

        # Extract metrics
        accuracies = [t['topk_partial_accuracy'] for t in trials if 'topk_partial_accuracy' in t]
        queries = [t['db_queries'] for t in trials if 'db_queries' in t]
        times = [t['total_time'] for t in trials if 'total_time' in t]

        acc_stats = compute_stats(accuracies)
        query_stats = compute_stats(queries)
        time_stats = compute_stats(times)

        summary.append({
            'engine': engine,
            'dataset': dataset,
            'n': n,
            'k': k,
            'mode': mode,
            'trials': len(trials),
            'acc_mean': acc_stats['mean'],
            'acc_std': acc_stats['std'],
            'queries_mean': query_stats['mean'],
            'queries_std': query_stats['std'],
            'time_mean': time_stats['mean'],
            'time_std': time_stats['std'],
        })

    return summary

def generate_comparison_table(summary):
    """Generate baseline vs G-DBREACH comparison"""
    comparisons = []

    # Group by (engine, dataset, n, k)
    configs = defaultdict(dict)
    for s in summary:
        key = (s['engine'], s['dataset'], s['n'], s['k'])
        configs[key][s['mode']] = s

    for key, modes in sorted(configs.items()):
        engine, dataset, n, k = key

        baseline = modes.get('baseline', {})
        gdbreach = modes.get('gdbreach', {})

        # Compute speedup
        baseline_queries = baseline.get('queries_mean', 0)
        gdbreach_queries = gdbreach.get('queries_mean', 0)
        speedup = baseline_queries / gdbreach_queries if gdbreach_queries > 0 else 0

        comparisons.append({
            'engine': engine,
            'dataset': dataset,
            'n': n,
            'k': k,
            'baseline_acc': baseline.get('acc_mean', 0),
            'gdbreach_acc': gdbreach.get('acc_mean', 0),
            'baseline_queries': baseline_queries,
            'gdbreach_queries': gdbreach_queries,
            'speedup': speedup,
            'baseline_time': baseline.get('time_mean', 0),
            'gdbreach_time': gdbreach.get('time_mean', 0),
        })

    return comparisons

def format_markdown_table(headers, rows, title=""):
    """Format data as markdown table"""
    output = []

    if title:
        output.append(f"\n## {title}\n")

    # Header
    output.append("| " + " | ".join(headers) + " |")
    output.append("| " + " | ".join(["---"] * len(headers)) + " |")

    # Rows
    for row in rows:
        output.append("| " + " | ".join(str(v) for v in row) + " |")

    return "\n".join(output)

def format_latex_table(headers, rows, title=""):
    """Format data as LaTeX table"""
    output = []

    if title:
        output.append(f"% {title}")

    output.append("\\begin{tabular}{" + "c" * len(headers) + "}")
    output.append("\\hline")
    output.append(" & ".join(headers) + " \\\\")
    output.append("\\hline")

    for row in rows:
        output.append(" & ".join(str(v) for v in row) + " \\\\")

    output.append("\\hline")
    output.append("\\end{tabular}")

    return "\n".join(output)

def print_summary_table(summary, fmt="markdown"):
    """Print summary table"""
    headers = ["Engine", "Dataset", "n", "k", "Mode", "Trials", "Accuracy", "Queries", "Time (s)"]

    rows = []
    for s in summary:
        rows.append([
            s['engine'],
            s['dataset'],
            s['n'],
            s['k'],
            s['mode'],
            s['trials'],
            f"{s['acc_mean']:.3f}",  # ± {s['acc_std']:.3f}",
            f"{s['queries_mean']:.0f}",  # ± {s['queries_std']:.0f}",
            f"{s['time_mean']:.1f}",  # ± {s['time_std']:.1f}",
        ])

    if fmt == "markdown":
        print(format_markdown_table(headers, rows, "K-of-N Attack Results Summary"))
    elif fmt == "latex":
        print(format_latex_table(headers, rows, "K-of-N Attack Results Summary"))

def print_comparison_table(comparisons, fmt="markdown"):
    """Print baseline vs G-DBREACH comparison"""
    headers = ["Engine", "Dataset", "n", "k", "Base Acc", "G-DB Acc", "Base Q", "G-DB Q", "Speedup"]

    rows = []
    for c in comparisons:
        if c['baseline_queries'] > 0 or c['gdbreach_queries'] > 0:
            rows.append([
                c['engine'],
                c['dataset'],
                c['n'],
                c['k'],
                f"{c['baseline_acc']:.3f}",
                f"{c['gdbreach_acc']:.3f}",
                f"{c['baseline_queries']:.0f}",
                f"{c['gdbreach_queries']:.0f}",
                f"{c['speedup']:.2f}x",
            ])

    if fmt == "markdown":
        print(format_markdown_table(headers, rows, "Baseline vs G-DBREACH Comparison"))
    elif fmt == "latex":
        print(format_latex_table(headers, rows, "Baseline vs G-DBREACH Comparison"))

def main():
    args = parse_args()

    # Load results
    print(f"Loading results from {args.input_dir}...")
    results = load_all_results(args.input_dir)

    if not results:
        print("[ERROR] No results found!")
        sys.exit(1)

    print(f"Loaded {len(results)} successful trials")

    # Group and analyze
    grouped = group_results(results)
    summary = generate_summary_table(grouped)
    comparisons = generate_comparison_table(summary)

    # Print tables
    print("\n" + "=" * 60)
    print_summary_table(summary, args.format)
    print("\n")
    print_comparison_table(comparisons, args.format)

    # Save to file if output_dir specified
    if args.output_dir:
        os.makedirs(args.output_dir, exist_ok=True)

        # Save summary CSV
        summary_path = os.path.join(args.output_dir, "summary.csv")
        with open(summary_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=summary[0].keys() if summary else [])
            writer.writeheader()
            writer.writerows(summary)
        print(f"\nSummary saved to {summary_path}")

        # Save comparison CSV
        if comparisons:
            comp_path = os.path.join(args.output_dir, "comparison.csv")
            with open(comp_path, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=comparisons[0].keys())
                writer.writeheader()
                writer.writerows(comparisons)
            print(f"Comparison saved to {comp_path}")

if __name__ == "__main__":
    main()

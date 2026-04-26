from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from scipy import stats


def load_dataset(dataset_path: Path) -> pd.DataFrame:
    df = pd.read_csv(dataset_path)
    df["install_time"] = pd.to_datetime(df["install_time"], errors="coerce")
    df["payment_time"] = pd.to_datetime(df["payment_time"], errors="coerce")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
    return df


def build_user_metrics(df: pd.DataFrame) -> pd.DataFrame:
    user_metrics = (
        df.groupby("user_id", as_index=False)
        .agg(
            ab_group=("ab_group", "first"),
            install_time=("install_time", "first"),
            total_amount=("amount", "sum"),
            payment_count=("payment_time", "count"),
        )
        .sort_values(["ab_group", "user_id"])
    )
    user_metrics["is_paying"] = user_metrics["payment_count"] > 0
    return user_metrics


def build_group_metrics(user_metrics: pd.DataFrame) -> pd.DataFrame:
    metrics = (
        user_metrics.groupby("ab_group", as_index=False)
        .agg(
            users=("user_id", "count"),
            conversion_rate=("is_paying", "mean"),
            arpu=("total_amount", "mean"),
            total_revenue=("total_amount", "sum"),
        )
    )

    arppu = (
        user_metrics[user_metrics["is_paying"]]
        .groupby("ab_group", as_index=False)
        .agg(arppu=("total_amount", "mean"))
    )

    metrics = metrics.merge(arppu, on="ab_group", how="left")
    metrics["arppu"] = metrics["arppu"].fillna(0)
    return metrics.rename(columns={"ab_group": "group"})


def run_stat_tests(user_metrics: pd.DataFrame) -> pd.DataFrame:
    group_1 = user_metrics[user_metrics["ab_group"] == 1]
    group_2 = user_metrics[user_metrics["ab_group"] == 2]

    contingency_table = [
        [
            int(group_1["is_paying"].sum()),
            int((~group_1["is_paying"]).sum()),
        ],
        [
            int(group_2["is_paying"].sum()),
            int((~group_2["is_paying"]).sum()),
        ],
    ]

    _, p_value_cr, _, _ = stats.chi2_contingency(contingency_table)
    _, p_value_arpu = stats.mannwhitneyu(
        group_1["total_amount"],
        group_2["total_amount"],
        alternative="two-sided",
    )
    _, p_value_arppu = stats.mannwhitneyu(
        group_1.loc[group_1["is_paying"], "total_amount"],
        group_2.loc[group_2["is_paying"], "total_amount"],
        alternative="two-sided",
    )

    return pd.DataFrame(
        {
            "metric": ["CR", "ARPU", "ARPPU"],
            "p_value": [p_value_cr, p_value_arpu, p_value_arppu],
        }
    )


def plot_metrics(metrics: pd.DataFrame, output_path: Path | None = None) -> None:
    sns.set_theme(style="whitegrid")
    palette = {1: "#7f8c8d", 2: "#2980b9"}

    fig, axes = plt.subplots(1, 3, figsize=(16, 5))
    plot_columns = [
        ("conversion_rate", "Conversion Rate"),
        ("arpu", "ARPU"),
        ("arppu", "ARPPU"),
    ]

    for axis, (column, title) in zip(axes, plot_columns):
        sns.barplot(
            data=metrics,
            x="group",
            y=column,
            hue="group",
            palette=palette,
            legend=False,
            ax=axis,
        )
        axis.set_title(title)
        axis.set_xlabel("Group")
        axis.set_ylabel(column.upper())

    fig.tight_layout()

    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        print(f"Chart saved to: {output_path}")
        return

    plt.show()


def print_report(metrics: pd.DataFrame, tests: pd.DataFrame, user_metrics: pd.DataFrame) -> None:
    print(f"Users total: {len(user_metrics)}")
    print()
    print("Group metrics:")
    print(metrics.round(4).to_string(index=False))
    print()
    print("Statistical tests:")
    print(tests.round(6).to_string(index=False))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a compact A/B test analysis.")
    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path("data/task2_ab_dataset.csv"),
        help="Path to the A/B dataset CSV file.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional path to save the metrics chart.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    df = load_dataset(args.dataset)
    user_metrics = build_user_metrics(df)
    metrics = build_group_metrics(user_metrics)
    tests = run_stat_tests(user_metrics)

    print_report(metrics, tests, user_metrics)
    plot_metrics(metrics, args.output)


if __name__ == "__main__":
    main()

"""
SciPulse — Exécution d'une suite Great Expectations (GX ≥ 1.0) sur un DataFrame.

Mode « fichier » : le contexte vit dans `gx/` (non versionné) ;
les Data Docs HTML sont générés dans `gx/uncommitted/data_docs/`.
"""

from pathlib import Path

import great_expectations as gx
import pandas as pd
from great_expectations.checkpoint import UpdateDataDocsAction

GX_ROOT = Path(__file__).resolve().parents[2]


def run_suite(name: str, df: pd.DataFrame, expectations: list, build_docs: bool = True):
    """
    Valide `df` avec la liste d'expectations. Retourne le CheckpointResult.

    `result.success` indique si TOUTES les expectations sont satisfaites.
    """
    context = gx.get_context(mode="file", project_root_dir=str(GX_ROOT))

    ds = context.data_sources.add_or_update_pandas(f"{name}_pandas")
    try:
        asset = ds.get_asset(f"{name}_data")
    except LookupError:
        asset = ds.add_dataframe_asset(f"{name}_data")
    try:
        batch_def = asset.get_batch_definition("whole_dataframe")
    except KeyError:
        batch_def = asset.add_batch_definition_whole_dataframe("whole_dataframe")

    suite = context.suites.add_or_update(gx.ExpectationSuite(name=f"{name}_suite"))
    suite.expectations = []
    for expectation in expectations:
        suite.add_expectation(expectation)
    suite.save()

    validation = context.validation_definitions.add_or_update(
        gx.ValidationDefinition(name=f"{name}_validation", data=batch_def, suite=suite)
    )
    checkpoint = context.checkpoints.add_or_update(
        gx.Checkpoint(
            name=f"{name}_checkpoint",
            validation_definitions=[validation],
            actions=[UpdateDataDocsAction(name="update_data_docs")] if build_docs else [],
        )
    )
    return checkpoint.run(batch_parameters={"dataframe": df})


def print_report(result) -> None:
    """Affiche un résumé lisible expectation par expectation."""
    for vr in result.run_results.values():
        stats = vr.statistics
        print(
            f"\n  {stats['successful_expectations']}/{stats['evaluated_expectations']} "
            f"expectations satisfaites ({stats['success_percent']:.0f} %)"
        )
        for r in vr.results:
            cfg = r.expectation_config
            col = cfg.kwargs.get("column", "(table)")
            mark = "✅" if r.success else "❌"
            pct = r.result.get("unexpected_percent")
            extra = f" — {pct:.2f} % non conformes" if pct else ""
            print(f"   {mark} {cfg.type:<55} {col}{extra}")
    print(f"\n  {'🎉 VALIDATION RÉUSSIE' if result.success else '🚨 VALIDATION ÉCHOUÉE'}")

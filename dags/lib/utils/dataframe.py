"""Generic pandas helpers."""


def normalize_columns(df):
    """Lower-case and snake-case column names (e.g. 'BioGRID ID Interactor A' -> 'biogrid_id_interactor_a')."""
    return df.rename(
        lambda column_name: column_name.lower().replace(' ', '_').replace('#', '_').strip('_'),
        axis='columns',
    )

"""DatabaseFeatures for PyroSQL."""

from django.db.backends.base.features import BaseDatabaseFeatures
from django.utils.functional import cached_property


class DatabaseFeatures(BaseDatabaseFeatures):
    minimum_database_version = None

    allows_group_by_selected_pks = True
    can_return_columns_from_insert = True
    can_return_rows_from_bulk_insert = True
    has_real_datatype = True
    has_native_uuid_field = True
    has_native_json_field = True
    has_bulk_insert = True
    supports_tablespaces = False
    supports_sequence_reset = True
    can_introspect_default = True
    can_introspect_foreign_keys = True
    can_introspect_autofield = True
    can_introspect_ip_address_field = False
    can_introspect_materialized_views = False
    can_introspect_small_integer_field = True
    can_distinct_on_fields = True
    can_rollback_ddl = True
    supports_combined_alters = True
    supports_partial_indexes = True
    supports_expression_indexes = True
    supports_covering_indexes = False
    supports_deferrable_unique_constraints = True
    supports_comments = True
    supports_comments_inline = False
    has_select_for_update = True
    has_select_for_update_nowait = True
    has_select_for_update_skip_locked = True
    has_select_for_update_of = True
    can_defer_constraint_checks = True
    supports_over_clause = True
    supports_frame_range_fixed_distance = True
    only_supports_unbounded_with_preceding_and_following = False
    supports_aggregate_filter_clause = True
    supports_order_by_nulls_modifier = True
    order_by_nulls_first = False
    supports_json_field_contains = True
    supports_collation_on_charfield = True
    supports_collation_on_textfield = True
    supports_non_deterministic_collations = False
    supports_update_conflicts = True
    supports_update_conflicts_with_target = True
    supports_boolean_expr_in_select_clause = True
    supports_explaining_query_execution = True
    supports_default_keyword_in_insert = True
    supports_default_keyword_in_bulk_insert = True
    supports_unspecified_pk = True
    test_collations = {
        "ci": "und-x-icu",
        "non_default": "sv-x-icu",
        "swedish_ci": "sv-x-icu",
    }
    uses_savepoints = True
    can_alter_table_rename_column = True
    can_alter_table_drop_column = True
    can_introspect_check_constraints = True
    can_introspect_duration_field = False
    max_query_params = None

    @cached_property
    def django_test_skips(self):
        return {}

    @cached_property
    def introspected_field_types(self):
        return {
            "AutoField": "AutoField",
            "BigAutoField": "BigAutoField",
            "SmallAutoField": "SmallAutoField",
            "BooleanField": "BooleanField",
            "CharField": "CharField",
            "DateField": "DateField",
            "DateTimeField": "DateTimeField",
            "DecimalField": "DecimalField",
            "FloatField": "FloatField",
            "IntegerField": "IntegerField",
            "BigIntegerField": "BigIntegerField",
            "SmallIntegerField": "SmallIntegerField",
            "IPAddressField": "IPAddressField",
            "GenericIPAddressField": "GenericIPAddressField",
            "JSONField": "JSONField",
            "PositiveBigIntegerField": "BigIntegerField",
            "PositiveIntegerField": "IntegerField",
            "PositiveSmallIntegerField": "SmallIntegerField",
            "SlugField": "SlugField",
            "TextField": "TextField",
            "TimeField": "TimeField",
            "UUIDField": "UUIDField",
        }

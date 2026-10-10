"""The argument surface the report commands share.

A :class:`ReportSource` pairs one argument group with the validation that
group needs; :func:`configure_report_parser` and
:func:`validate_report_arguments` then add and check a whole report
command's arguments. Everything here composes what
:mod:`app.cli.arguments` and :mod:`app.cli.validation` provide, and
neither of those modules knows about this one.
"""

import argparse
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Final

from app.cli.arguments import (
    ArgumentGroup,
    add_email_argument_group,
    add_es_argument_group,
    add_jupyterhub_argument_group,
    add_output_argument_group,
    add_query_argument_group,
)
from app.cli.validation import (
    reject_es_flags_without_endpoint,
    validate_email_arguments,
    validate_es_arguments,
    validate_jupyterhub_arguments,
    validate_query_arguments,
)


def _add_optional_es_argument_group(
    parser: argparse.ArgumentParser,
) -> ArgumentGroup:
    """Add an "Elasticsearch" argument group whose flags are all optional."""
    return add_es_argument_group(parser, required=False)


def _validate_optional_es_arguments(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> None:
    """Validate an optional Elasticsearch group.

    ``--es-endpoint`` is the switch: without it, Elasticsearch is not
    used, and any other flag in the group is an error. With it, the rest
    of the group must be complete. Either way, a half-specified connection
    is an error rather than a silent skip.
    """
    reject_es_flags_without_endpoint(args, parser)
    if args.es_endpoint is None:
        return
    if args.es_index is None:
        parser.error("--es-index is required when --es-endpoint is given")
    validate_es_arguments(args, parser)


@dataclass(frozen=True)
class ReportSource:
    """A data source that a report command draws its arguments from.

    Each instance ties an argument group to the validation that group
    needs, so the two cannot be paired up wrongly at a call site. The
    constants below are the whole set; a command names the ones it uses
    in both :func:`configure_report_parser` and
    :func:`validate_report_arguments`.

    ``add_arguments`` returns the group it added, which nothing here
    needs; a command that wants it calls the ``add_*`` helper directly.
    """

    add_arguments: Callable[[argparse.ArgumentParser], object]
    validate: Callable[[argparse.Namespace, argparse.ArgumentParser], None]


JUPYTERHUB_SOURCE: Final = ReportSource(
    add_arguments=add_jupyterhub_argument_group,
    validate=validate_jupyterhub_arguments,
)

ELASTICSEARCH_SOURCE: Final = ReportSource(
    add_arguments=add_es_argument_group,
    validate=validate_es_arguments,
)

OPTIONAL_ELASTICSEARCH_SOURCE: Final = ReportSource(
    add_arguments=_add_optional_es_argument_group,
    validate=_validate_optional_es_arguments,
)


def configure_report_parser(
    parser: argparse.ArgumentParser,
    *,
    sources: Sequence[ReportSource],
    subject_prefix: str,
    include_date_format: bool = True,
    include_anonymize: bool = False,
) -> ArgumentGroup:
    """Configure shared report command argument groups.

    Each source in *sources* adds its group in the order given, ahead of
    the shared "Query", "Output", and "Email" groups. The "Query" group is
    returned so a command can extend it, as both report commands do with
    ``--hub``.

    The ``include_*`` switches let a report command omit an optional
    argument, which also means it must stop reading the corresponding
    attribute. ``get-new-activity`` clears *include_date_format*, since a
    bare date would leave its ``Started`` column naming no one session,
    so only ``get-new-users`` reaches
    :func:`app.cli.factories.get_strftime_fmt`. Both commands set
    *include_anonymize*, so the builders read ``args.anonymize``
    unconditionally.
    """
    for source in sources:
        source.add_arguments(parser)

    query_group = add_query_argument_group(parser)
    add_output_argument_group(
        parser,
        include_date_format=include_date_format,
        include_anonymize=include_anonymize,
    )
    add_email_argument_group(parser, subject_prefix=subject_prefix)
    return query_group


def validate_report_arguments(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
    *,
    sources: Sequence[ReportSource],
) -> None:
    """Run shared validation for report commands.

    *sources* must be the ones given to :func:`configure_report_parser`,
    in the same order: a source validates the arguments it added.
    """
    for source in sources:
        source.validate(args, parser)
    validate_query_arguments(args, parser)
    validate_email_arguments(args, parser)

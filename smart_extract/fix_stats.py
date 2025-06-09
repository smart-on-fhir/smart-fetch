import dataclasses

import rich.table


@dataclasses.dataclass()
class FixStats:
    total: int = 0
    already_done: int = 0
    newly_done: int = 0
    fatal_errors: int = 0
    retry_errors: int = 0

    def print(self, header: str, adjective: str):
        table = rich.table.Table(
            "",
            rich.table.Column(header=header, justify="right"),
            box=None,
        )
        table.add_row("Total examined", f"{self.total:,}")
        if self.already_done:
            table.add_row(f"Already {adjective}", f"{self.already_done:,}")
        table.add_row(f"Newly {adjective}", f"{self.newly_done:,}")
        if self.fatal_errors:
            table.add_row("Fatal errors", f"{self.fatal_errors:,}")
        if self.retry_errors:
            table.add_row("Retried but gave up", f"{self.retry_errors:,}",)
        rich.get_console().print(table)

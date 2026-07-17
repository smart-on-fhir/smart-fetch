import io
import unittest
from unittest import mock

import rich.console

from smart_fetch import cli_utils, iter_utils


class ProgressTests(unittest.TestCase):
    @mock.patch("smart_fetch.cli_utils.Progress")
    def test_make_progress_bar_returns_progress(self, mock_progress):
        self.assertIs(cli_utils.make_progress_bar(), mock_progress.return_value)
        mock_progress.assert_called_once_with()

    @mock.patch("smart_fetch.cli_utils._RefreshThread")
    @mock.patch("rich.get_console")
    def test_redirected_output_starts_refresh_thread(self, mock_get_console, mock_thread):
        console = rich.console.Console(file=io.StringIO(), force_terminal=False)
        mock_get_console.return_value = console

        progress = cli_utils.Progress()
        task_id = progress.add_task("Crawling Patients", total=4)

        mock_thread.assert_called_once_with(progress=progress, task_id=task_id)
        mock_thread.return_value.start.assert_called_once_with()
        mock_thread.return_value.task_id = task_id

        progress.update(task_id, advance=1)
        mock_thread.return_value.stop.assert_not_called()

        progress.update(task_id, completed=4, total=4)
        mock_thread.return_value.stop.assert_called_once_with()
        self.assertIsNone(progress._thread)
        progress.stop()

    @mock.patch("smart_fetch.cli_utils._RefreshThread")
    @mock.patch("rich.get_console")
    def test_interactive_output_does_not_start_refresh_thread(self, mock_get_console, mock_thread):
        console = rich.console.Console(file=io.StringIO(), force_terminal=True)
        mock_get_console.return_value = console

        progress = cli_utils.Progress()
        progress.add_task("Crawling Patients", total=4)

        mock_thread.assert_not_called()
        progress.stop()

    @mock.patch("rich.print")
    @mock.patch("rich.get_console")
    def test_refresh_message_includes_progress_and_elapsed_time(self, mock_get_console, mock_print):
        console = rich.console.Console(file=io.StringIO(), force_terminal=True)
        mock_get_console.return_value = console
        progress = cli_utils.Progress()
        task_id = progress.add_task("Crawling Patients", total=4, completed=1)
        task = progress.tasks[0]
        refresh = cli_utils._RefreshThread(progress, task_id)

        refresh._print()
        mock_print.assert_called_once_with("Crawling Patients…")

        mock_print.reset_mock()
        task.start_time -= 90
        refresh._print()
        mock_print.assert_called_once_with("Crawling Patients… (25%, 1.5m so far)")

        mock_print.reset_mock()
        task.total = None
        refresh._print()
        mock_print.assert_called_once_with("Crawling Patients… (1.5m so far)")

        self.assertEqual(refresh._delay(), 60)
        task.start_time -= 60 * 60
        self.assertEqual(refresh._delay(), 120)
        refresh.stop()
        self.assertTrue(refresh.done.is_set())
        progress.stop()

    @mock.patch("rich.get_console")
    def test_refresh_thread_prints_before_waiting(self, mock_get_console):
        console = rich.console.Console(file=io.StringIO(), force_terminal=True)
        mock_get_console.return_value = console
        progress = cli_utils.Progress()
        task_id = progress.add_task("Crawling Patients", total=4)
        refresh = cli_utils._RefreshThread(progress, task_id)
        refresh.done = mock.Mock()
        refresh.done.wait.side_effect = [False, True]
        refresh.done.is_set.return_value = False

        with mock.patch.object(refresh, "_print") as mock_print:
            refresh.run()

        self.assertEqual(mock_print.call_count, 2)
        self.assertEqual(refresh.done.wait.call_args_list, [mock.call(60), mock.call(60)])
        progress.stop()


class ResourceProcessorTests(unittest.IsolatedAsyncioTestCase):
    @mock.patch("smart_fetch.iter_utils.peek_ahead_processor", new_callable=mock.AsyncMock)
    @mock.patch("smart_fetch.iter_utils.ndjson.NdjsonWriter")
    @mock.patch("smart_fetch.iter_utils.cli_utils.Progress")
    async def test_run_uses_redirected_progress(
        self, mock_progress_cls, mock_writer_cls, mock_peek_ahead
    ):
        progress = mock_progress_cls.return_value.__enter__.return_value
        progress.add_task.return_value = 7
        processor = iter_utils.ResourceProcessor(".", "Crawling", mock.AsyncMock())
        processor.add_source("Patient", mock.Mock(), total=3, output_file="patients.ndjson")

        await processor.run()

        mock_progress_cls.assert_called_once_with()
        progress.add_task.assert_called_once_with("Crawling Patients", total=3)
        mock_writer_cls.assert_called_once()
        mock_peek_ahead.assert_awaited_once()
        self.assertEqual(processor.sources, {})

    async def test_processed_item_advances_progress(self):
        callback = mock.AsyncMock()
        processor = iter_utils.ResourceProcessor(".", "Crawling", callback)
        progress = mock.Mock()
        writer = mock.Mock()
        item = {"id": "example"}

        await processor._process_wrapper(writer, "Patient", progress, 3, item)

        callback.assert_awaited_once_with("Patient", writer, item)
        progress.advance.assert_called_once_with(3)

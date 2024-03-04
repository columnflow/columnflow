"""
Custom law task method decorators.
"""

import law
from typing import Any, Callable


@law.decorator.factory(accept_generator=True)
def view_output_plots(
    fn: Callable,
    opts: Any,
    task: law.Task,
    *args: Any,
    **kwargs: Any,
) -> tuple[Callable, Callable, Callable]:
    """
    Decorator to view output plots.

    This decorator is used to view the output plots of a task. It checks if the task has a view command,
    collects all the paths of the output files, and then opens each file using the view command.

    :param fn: The function to be decorated.
    :param opts: Options for the decorator.
    :param task: The task instance.
    :param args: Variable length argument list.
    :param kwargs: Arbitrary keyword arguments.
    :return: A tuple containing the before_call, call, and after_call functions.
    """

    def before_call() -> None:
        """
        Function to be called before the decorated function.

        :return: None
        """
        return None

    def call(state: Any) -> Any:
        """
        The decorated function.

        :param state: The state of the task.
        :return: The result of the decorated function.
        """
        return fn(task, *args, **kwargs)

    def after_call(state: Any) -> None:
        """
        Function to be called after the decorated function.

        :param state: The state of the task.
        :return: None
        """
        view_cmd = getattr(task, "view_cmd", None)
        if not view_cmd or view_cmd == law.NO_STR:
            return

        # prepare the view command
        if "{}" not in view_cmd:
            view_cmd += " {}"

        # collect all paths to view
        view_paths: list[str] = []
        outputs: list[Any] = law.util.flatten(task.output())
        while outputs:
            output = outputs.pop(0)
            if isinstance(output, law.TargetCollection):
                outputs.extend(output._flat_target_list)
                continue
            if not getattr(output, "path", None):
                continue
            if output.path.endswith((".pdf", ".png")):
                if not isinstance(output, law.LocalTarget):
                    task.logger.warning(f"cannot show non-local plot at '{output.path}'")
                    continue
                elif output.path not in view_paths:
                    view_paths.append(output.path)

        # loop through paths and view them
        for path in view_paths:
            task.publish_message("showing {}".format(path))
            law.util.interruptable_popen(view_cmd.format(path), shell=True, executable="/bin/bash")

    return before_call, call, after_call

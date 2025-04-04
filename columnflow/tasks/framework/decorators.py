"""
Custom law task method decorators.
"""

import law

from columnflow.types import Any, Callable


@law.decorator.factory(callback=None, accept_generator=True)
def on_failure(
    fn: Callable,
    opts: Any,
    task: law.Task,
    *args: Any,
    **kwargs: Any,
) -> tuple[Callable, Callable, Callable]:
    """ callback(callback=None)
    A decorator that is configured with a callback function that is invoked if the decorated method raises an exception.
    The task instances is passed to the callback function as an argument.

    :param fn: The decorated function.
    :param opts: Options for the decorator.
    :param task: The task instance.
    :param args: Arguments to be passed to the function call.
    :param kwargs: Keyword arguments to be passed to the function call.
    :return: A tuple containing the before_call, call, and after_call functions.
    """
    def before_call() -> None:
        return None

    def call(state: Any) -> Any:
        try:
            return fn(task, *args, **kwargs)
        except:
            if callable(opts["callback"]):
                try:
                    opts["callback"](task)
                except Exception as e:
                    task.logger.error(f"failure callback raised an exception: {e}")
            raise

    def after_call(state: Any) -> None:
        return None

    return before_call, call, after_call


@law.decorator.factory(accept_generator=True)
def view_output_plots(
    fn: Callable,
    opts: Any,
    task: law.Task,
    *args: Any,
    **kwargs: Any,
) -> tuple[Callable, Callable, Callable]:
    """ view_output_plots()
    This decorator is used to view the output plots of a task. It checks if the task has a view command, collects all
    the paths of the output files, and then opens each file using the view command.

    :param fn: The decorated function.
    :param opts: Options for the decorator.
    :param task: The task instance.
    :param args: Arguments to be passed to the function call.
    :param kwargs: Keyword arguments to be passed to the function call.
    :return: A tuple containing the before_call, call, and after_call functions.
    """
    def before_call() -> None:
        return None

    def call(state: Any) -> Any:
        return fn(task, *args, **kwargs)

    def after_call(state: Any) -> None:
        view_cmd = getattr(task, "view_cmd", None)
        if not view_cmd or view_cmd == law.NO_STR:
            return

        # prepare the view command
        if "{}" not in view_cmd:
            view_cmd += " {}"

        # collect all paths to view
        seen: set[str] = set()
        outputs: list[Any] = law.util.flatten(task.output())
        while outputs:
            output = outputs.pop(0)
            if isinstance(output, law.TargetCollection):
                outputs.extend(output._flat_target_list)
                continue
            if not getattr(output, "abspath", None):
                continue
            path = output.abspath
            if not path.endswith((".pdf", ".png")):
                continue
            if path in seen:
                continue
            seen.add(path)
            task.publish_message(f"showing {path}")
            with output.localize("r") as tmp:
                law.util.interruptable_popen(
                    view_cmd.format(tmp.abspath),
                    shell=True,
                    executable="/bin/bash",
                )

    return before_call, call, after_call

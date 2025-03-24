import time
from collections import defaultdict
import functools

prev_timing_dict = defaultdict(bool)
timing_dict = defaultdict(bool)
prev_counting_dict = defaultdict(bool)
counting_dict = defaultdict(bool)


def update_timing(key, dt, min_dt=5, count=False, min_count=10):
    timing_dict[key] += dt
    if timing_dict[key] - prev_timing_dict[key] > min_dt:
        print(key, timing_dict[key])
        prev_timing_dict[key] = timing_dict[key]

    if count:
        counting_dict[key] += 1
        if counting_dict[key] - prev_counting_dict[key] > min_count:
            print(key, counting_dict[key])
            prev_counting_dict[key] = counting_dict[key]


def timer(func=None, *, tag=None, min_dt=5, count=False, min_count=10):
    """
    decorator to time a function call.
    Total time is printed to terminal (time per call X number of calls).
    Several functions can be grouped by providing a tag.
    Also the number of calls can be printed if *count=True*
    The minimum time / number of calls to trigger a print can also be specified

    @timer
    def my_func():
        ...

    OUT: my_func <time if time>

    @timer(tag="tag")
    def my_func():
        ...

    OUT: tag <time> (sum of all functions with this tag)
    OUT: tag my_func <time>


    """
    wrap_kwargs = dict(minD_dt=min_dt, count=count, min_count=min_count)
    def outer(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            t0 = time.time()
            out = f(*args, **kwargs)
            dt = time.time() - t0
            if tag is not None:
                update_timing(tag, dt, **wrap_kwargs)
                update_timing((tag, f.__name__), dt, **wrap_kwargs)
            else:
                update_timing(f.__name__, dt, **wrap_kwargs)
            return out
        return wrapper
    return outer if func is None else outer(func)


class Timer:
    """
    Class to create timer objects.
    Example:

    tmr = Timer("my_timer")  # at t0
    ...
    tmr("checkpoint 1") # at t1
    OUT: my_timer checkpoint 1 t1 - t0
    ...
    tmr("checkpoint 2") # at t2
    OUT: my_timer checkpoint 2 t2 - t1

    """
    def __init__(self, tag):
        self.t0 = self.tp = time.time()
        self.dt = 5
        self.tag = "\033[94m" + tag + "\033[0m"
        print("\033[1msetup timer", tag, "\033[0m")

    def __call__(self, tag=None, force=False):
        t = time.time()
        # if (t := time.time()) - self.tp > self.dt:
        tag = self.tag + ("" if tag is None else f"\033[96m {tag}\033[0m")
        if (t - self.tp > 0.01) or force:
            print(tag, t - self.tp)
        self.tp = t

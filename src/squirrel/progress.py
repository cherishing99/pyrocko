from __future__ import absolute_import, print_function

import time

from .get_terminal_size import get_terminal_size


spinner = u'\u25dc\u25dd\u25de\u25df'
skull = u'\u2620'
check = u'\u2714'
ansi_up = '\033[%iA'


class DynWindow(object):
    def __init__(self, height):
        self._height = height

    def start(self):
        print('\033[2J\033[1;%ir\033[%i;1H'
              % (self.nlines_scroll, self.nlines_scroll-1), end=None)
        print('\033[%i;1H\033[0J' % (self.nlines_scroll+1), end=None)



    def 
        print('\033[2J\033[1;%ir\033[%i;1H'
              % (self.nlines_scroll, self.nlines_scroll-1), end=None)
        return self

    def __exit__(self, type, value, traceback):
        print('\033[r\033[%i;1H\033[0J\033[%i;1H'
              % (self.nlines_scroll+1, self.nlines_scroll-1))

    def _start_show(self):

    def _end_show(self):
        print('\033[%i;1H' % (self.nlines_scroll-1), end=None)

    def show(self, s):
        self._start_show()
        print(s, end=None)
        self._end_show()


class Task(object):
    def __init__(self, progress, id, name, n):
        self._id = id
        self._name = name
        self._i = 0
        self._n = n
        self._done = False
        self._state = 'waiting'
        self._progress = progress

    def __call__(self, it):
        try:
            for i, obj in enumerate(it):
                self.update(i)
                yield obj

        finally:
            self.end()

    def update(self, i, condition=''):
        self._state = 'working'
        self._condition = condition
        self._i = i
        self._progress._update()

    def end(self, condition=check):
        self._condition = condition
        self._state = 'done'
        self._i += 1
        self._progress._end(self)

    def _str_state(self):
        s = self._state
        if s == 'waiting':
            return ' '
        elif s == 'working':
            return spinner[self._i % len(spinner)]
        elif s == 'done':
            return check
        elif s == 'error':
            return skull
        else:
            return '?'

    def _str_progress(self):
        if self._n is None:
            return '%i' % self._i
        else:
            return '%i / %i  %3.0f%%' % (
                self._i, self._n, float(self._i) / self._n)

    def __str__(self):
        return '%s %s: %s' % (
            self._str_state(),
            self._name,
            self._str_progress())


class Progress(object):

    def __init__(self):
        self._current_id = 0
        self._tasks = {}
        self._tasks_done = []
        self._last_update = 0.0

    def task(self, name, n=None):
        self._current_id += 1
        task = Task(self, self._current_id, name, n)
        self._tasks[task._id] = task
        self._update(force=True)
        return task

    def _end(self, task):
        self._tasks[task._id]
        self._tasks_done.append(task)
        self._update(force=True)

    def _update(self, force=False):
        now = time.time()
        if self._last_update + 0.1 < now or force:
            lines_done, lines = self._lines()
            for line in lines_done:
                print(line)

            for line in lines:
                print(line)

            print(ansi_up % len(lines))
            self._last_update = now

    def _lines(self):
        task_ids = sorted(self._tasks)
        lines_done = []
        for task in self._tasks_done:
            lines_done.append(str(task))

        self._tasks_done = []

        lines = []
        for task_id in task_ids:
            task = self._tasks[task_id]
            lines.append(str(task))

        return '\n'.join(lines)

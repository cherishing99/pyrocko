from __future__ import absolute_import, print_function

import time

from .get_terminal_size import get_terminal_size


spinner = u'\u25dc\u25dd\u25de\u25df'
skull = u'\u2620'
check = u'\u2714'
bar = u'[- ]'

ansi_up = u'\033[%iA'
ansi_down = u'\033[%iB'
ansi_left = u'\033[%iC'
ansi_right = u'\033[%iD'
ansi_next_line = u'\033E'

ansi_erase_display = u'\033[2J'
ansi_window = u'\033[%i;%ir'
ansi_move_to = u'\033[%i;%iH'

ansi_clear_down = u'\033[0J'
ansi_clear_up = u'\033[1J'
ansi_clear = u'\033[2J'

ansi_clear_right = u'\033[0K'

ansi_scroll_up = u'\033D'
ansi_scroll_down = u'\033M'

ansi_reset = u'\033c'


class TerminalStatusWindow(object):
    def __init__(self):
        self._terminal_size = get_terminal_size()
        self._height = 0
        self._state = 0

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *_):
        self.stop()

    def print(self, s):
        print(s, end='')

    def flush(self):
        print('', end='', flush=True)

    def start(self):
        sx, sy = self._terminal_size
        print()
        self.flush()
        self._state = 1

    def stop(self):
        sx, sy = self._terminal_size
        self._resize(0)
        self.print(ansi_move_to % (sy-self._height, 1))
        self.flush()
        self._state = 2

    def _start_show(self):
        sx, sy = self._terminal_size
        self.print(ansi_move_to % (sy-self._height+1, 1))

    def _end_show(self):
        sx, sy = self._terminal_size
        self.print(ansi_move_to % (sy-self._height, 1))
        self.print(ansi_clear_right)

    def _resize(self, height):
        sx, sy = self._terminal_size
        k = height - self._height
        if k > 0:
            self.print(ansi_scroll_up * k)
            self.print(ansi_window % (1, sy-height))
        if k < 0:
            self.print(ansi_window % (1, sy-height))
            self.print(ansi_scroll_down * abs(k))

        self._height = height

    def draw(self, lines):
        if self._state != 1:
            return

        self._terminal_size = get_terminal_size()
        nlines = len(lines)
        self._resize(nlines)
        self._start_show()

        for iline, line in enumerate(lines):
            self.print(ansi_clear_right + line)
            if iline != nlines - 1:
                self.print(ansi_next_line)

        self._end_show()
        self.flush()

    @property
    def active(self):
        return self._state == 1


class Task(object):
    def __init__(self, progress, id, name, n):
        self._id = id
        self._name = name
        self._condition = ''
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
        if self._n is not None:
            i = min(i, self._n)

        self._i = i
        self._progress._update()

    def end(self, condition=''):
        if self._state in ('done', 'failed'):
            return

        self._condition = condition
        self._state = 'done'
        self._progress._end(self)

    def fail(self, condition=''):
        self._condition = condition
        self._state = 'failed'
        self._progress._end(self)

    def _str_state(self):
        s = self._state
        if s == 'waiting':
            return ' '
        elif s == 'working':
            return spinner[self._i % len(spinner)]
        elif s == 'done':
            return check
        elif s == 'failed':
            return skull
        else:
            return '?'

    def _str_progress(self):
        if self._n is None:
            return '%i' % self._i
        else:
            return '%i / %i%s' % (
                self._i, self._n,
                '  %3.0f%%' % ((100. * self._i) / self._n)
                if self._state == 'working' else '')

    def _str_condition(self):
        if self._condition:
            return ' - %s' % self._condition
        else:
            return ''

    def _str_bar(self):
        if self._state == 'working' and self._n is not None:
            nb = 30
            ib = int(nb * float(self._i) / self._n)
            return ' ' + bar[0] + bar[1] * ib + bar[2] * (nb - ib) + bar[3]
        else:
            return ''

    def __str__(self):
        return '%s %s: %s' % (
            self._str_state(),
            self._name,
            ' '.join([
                self._str_progress(),
                self._str_condition(),
                self._str_bar()]))


class Progress(object):

    def __init__(self):
        self._current_id = 0
        self._tasks = {}
        self._tasks_done = []
        self._last_update = 0.0
        self._term = None

    @property
    def show_in_terminal(self):
        self._term = TerminalStatusWindow()
        return self._term

    def task(self, name, n=None):
        self._current_id += 1
        task = Task(self, self._current_id, name, n)
        self._tasks[task._id] = task
        self._update(force=True)
        return task

    def _end(self, task):
        del self._tasks[task._id]
        self._tasks_done.append(task)
        self._update(force=True)

    def _update(self, force=False):
        now = time.time()
        if self._last_update + 0.1 < now or force:
            lines_done, lines = self._lines()
            for line in lines_done:
                print(line)

            if self._term and self._term.active:
                self._term.draw(lines)

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

        return lines_done, lines

from __future__ import annotations
from contextlib import contextmanager
from dataclasses import dataclass, field, asdict
from enum import Enum
from sys import stdout
from time import time, sleep
import tomllib

import typer
import psycopg
import psycopg.sql
import psycopg.rows
from psycopg.adapt import Loader

SETUP = r"""
DROP FUNCTION IF EXISTS put(int[][], int, int, int) CASCADE;
DROP FUNCTION IF EXISTS get(int[][], int, int) CASCADE;
DROP FUNCTION IF EXISTS to_str(int) CASCADE;
DROP FUNCTION IF EXISTS from_str(text) CASCADE;
DROP FUNCTION IF EXISTS wrap(int, int) CASCADE;
DROP TYPE IF EXISTS direction CASCADE;
DROP TYPE IF EXISTS execution_mode CASCADE;


CREATE TYPE direction AS ENUM
  ('🡺', '🡻', '🡸', '🡹');


CREATE TYPE execution_mode AS ENUM
  ('⚙️', '🏃', '🏁', '🪡', '🚀');


CREATE FUNCTION wrap(i int, e int) RETURNS int AS $$
  SELECT 1 + (e + i - 1) % e;
$$ LANGUAGE SQL IMMUTABLE;


CREATE FUNCTION get(grid int[][], _y int, _x int) RETURNS int AS $$
  SELECT grid[y][x]
  FROM   (
    SELECT wrap(coalesce(_y, 0)+1, array_length(grid, 1)),
           wrap(coalesce(_x, 0)+1, array_length(grid, 2))) AS _(y, x);
$$ LANGUAGE SQL IMMUTABLE;


CREATE FUNCTION put(grid int[][], _y int, _x int, _v int) RETURNS int[][] AS $$
  SELECT array_agg(r ORDER BY Y′)
  FROM   (
    SELECT wrap(coalesce(_y, 0)+1, array_length(grid, 1)),
           wrap(coalesce(_x, 0)+1, array_length(grid, 2)),
           coalesce(_v, 0)                               ) AS _(y, x, v),
  LATERAL (
      SELECT   Y′, array_agg(CASE (X′,Y′) WHEN (x,y) THEN v ELSE grid[Y′][X′] END ORDER BY X′)
      FROM     generate_series(1,array_length(grid,1)) AS Y′,
               generate_series(1,array_length(grid,2)) AS X′
      GROUP BY Y′) AS __(Y′, r);
$$ LANGUAGE SQL IMMUTABLE;


CREATE FUNCTION to_str(c int) RETURNS text AS $$
  SELECT CASE coalesce(c, 0)
    WHEN 0 THEN '␀'
           ELSE chr(c)
  END;
$$ LANGUAGE SQL IMMUTABLE;


CREATE FUNCTION from_str(c text) RETURNS int AS $$
  SELECT CASE coalesce(c, '␀')
    WHEN '␀' THEN 0
             ELSE ascii(c)
  END;
$$ LANGUAGE SQL IMMUTABLE;
"""

RUNTIME = r"""
  WITH RECURSIVE
    -- preprocess takes the input program/playfield string and…
    --  1. ensures the correct dimensions
    --  2. transform it in to a 2D array
    preprocess(grid) AS (
      SELECT (
        (array_agg(
          string_to_array(
            rpad(l, {width}, ' '), NULL)
          )
        )[1:{height}][1:{width}])
      FROM regexp_split_to_table(
        {source} ||
        repeat(
          repeat(' ', {width}) || E'\n',
          {height}),
        '\n') WITH ORDINALITY AS l),

    preprocess′(grid) AS (
      SELECT  array_agg(r ORDER BY y)
      FROM    preprocess AS _(grid),
      LATERAL (
        SELECT y, array_agg(from_str(grid[y][x]) ORDER BY x)
        FROM   generate_series(1,{height}) AS y,
               generate_series(1,{width})  AS x
        GROUP BY y) AS __(y, r)),

    step("execution mode", "next execution mode", grid, input, output, direction, step_length, x, y, stack, steps) AS (
      SELECT '🚀' :: execution_mode, '⚙️' :: execution_mode, grid, {input} :: text[], '', '🡺' :: direction, 1, 1, 1, array[] :: int[], 0
      FROM   preprocess′ AS _(grid)
        UNION
      SELECT "next".*, s.steps + 1
      FROM    step AS s,
      LATERAL (SELECT to_str(s.grid[s.y][s.x]), {width}, {height}) AS _(c,w,h),
      LATERAL (
        SELECT s."next execution mode" :: execution_mode, null, s.grid, s.input, s.output, s.direction, 1, s.x, s.y, s.stack
        WHERE  s."execution mode" = '🚀'
          UNION
        SELECT _.*
        FROM   (
          SELECT '🏁' :: execution_mode, null :: execution_mode, s.grid, s.input, s.output, s.direction, 1, s.x, s.y, s.stack
          WHERE  c = '@'
            UNION
          SELECT '🏃', '⚙️', s.grid, s.input, s.output, '🡺', 1, s.x, s.y, s.stack
          WHERE  c = '>'
            UNION
          SELECT '🏃', '⚙️', s.grid, s.input, s.output, '🡻', 1, s.x, s.y, s.stack
          WHERE  c = 'v'
            UNION
          SELECT '🏃', '⚙️', s.grid, s.input, s.output, '🡸', 1, s.x, s.y, s.stack
          WHERE  c = '<'
            UNION
          SELECT '🏃', '⚙️', s.grid, s.input, s.output, '🡹', 1, s.x, s.y, s.stack
          WHERE  c = '^'
            UNION
          SELECT '🏃', '⚙️', s.grid, s.input, s.output, (array['🡺', '🡻', '🡸', '🡹'])[round(1 + random() * 3)] :: direction, 1, s.x, s.y, s.stack
          WHERE  c = '?'
            UNION
          SELECT '🏃', '⚙️', s.grid, s.input, s.output, s.direction, 2, s.x, s.y, s.stack
          WHERE  c = '#'
            UNION
          SELECT '🏃', '⚙️', s.grid, s.input, s.output, (array['🡸','🡺'])[1+(coalesce(s.stack[1], 0)=0)::int] :: direction, s.step_length, s.x, s.y, s.stack[2:]
          WHERE  c = '_'
            UNION
          SELECT '🏃', '⚙️', s.grid, s.input, s.output, (array['🡹','🡻'])[1+(coalesce(s.stack[1], 0)=0)::int] :: direction, s.step_length, s.x, s.y, s.stack[2:]
          WHERE  c = '|'
            UNION
          SELECT '🏃', '⚙️', s.grid, s.input, s.output, s.direction, 1, s.x, s.y, (coalesce(s.stack[2], 0) + coalesce(s.stack[1], 0)) || s.stack[3:]
          WHERE  c = '+'
            UNION
          SELECT '🏃', '⚙️', s.grid, s.input, s.output, s.direction, 1, s.x, s.y, (coalesce(s.stack[2], 0) - coalesce(s.stack[1], 0)) || s.stack[3:]
          WHERE  c = '-'
            UNION
          SELECT '🏃', '⚙️', s.grid, s.input, s.output, s.direction, 1, s.x, s.y, (coalesce(s.stack[2], 0) * coalesce(s.stack[1], 0)) || s.stack[3:]
          WHERE  c = '*'
            UNION
          SELECT '🏃', '⚙️', s.grid, s.input, s.output, s.direction, 1, s.x, s.y, (coalesce(s.stack[2], 0) / coalesce(s.stack[1], 0)) || s.stack[3:]
          WHERE  c = '/'
            UNION
          SELECT '🏃', '⚙️', s.grid, s.input, s.output, s.direction, 1, s.x, s.y, (coalesce(s.stack[2], 0) % coalesce(s.stack[1], 0)) || s.stack[3:]
          WHERE  c = '%'
            UNION
          SELECT '🏃', '⚙️', s.grid, s.input, s.output, s.direction, 1, s.x, s.y, (coalesce(s.stack[1], 0) = 0) :: int || s.stack[2:]
          WHERE  c = '!'
            UNION
          SELECT '🏃', '⚙️', s.grid, s.input, s.output, s.direction, 1, s.x, s.y, (coalesce(s.stack[2], 0) > coalesce(s.stack[1], 0)) :: int || s.stack[3:]
          WHERE  c = '`'
            UNION
          SELECT '🏃', '⚙️', s.grid, s.input, s.output || coalesce(s.stack[1], 0) :: text || ' ', s.direction, s.step_length, s.x, s.y, s.stack[2:]
          WHERE  c = '.'
            UNION
          SELECT '🏃', '⚙️', s.grid, s.input, s.output || to_str(coalesce(s.stack[1], 0)), s.direction, s.step_length, s.x, s.y, s.stack[2:]
          WHERE  c = ','
            UNION
          SELECT '🏃', '⚙️', s.grid, s.input[2:], s.output, s.direction, s.step_length, s.x, s.y, s.input[1] :: int || s.stack
          WHERE  c = '&'
            UNION
          SELECT '🏃', '⚙️', s.grid, s.input[2:], s.output, s.direction, s.step_length, s.x, s.y, from_str(s.input[1]) || s.stack
          WHERE  c = '~'
            UNION
          SELECT '🏃', '⚙️', s.grid, s.input, s.output, s.direction, s.step_length, s.x, s.y, s.stack[2:]
          WHERE  c = '$'
            UNION
          SELECT '🏃', '⚙️', s.grid, s.input, s.output, s.direction, s.step_length, s.x, s.y, coalesce(s.stack[1], 0) || s.stack
          WHERE  c = ':'
            UNION
          SELECT '🏃', '⚙️', s.grid, s.input, s.output, s.direction, s.step_length, s.x, s.y, array[coalesce(s.stack[2], 0), coalesce(s.stack[1], 0)] || s.stack[3:]
          WHERE  c = '\'
            UNION
          SELECT '🏃', '⚙️', s.grid, s.input, s.output, s.direction, s.step_length, s.x, s.y, get(s.grid, s.stack[1], s.stack[2]) || s.stack[3:]
          WHERE  c = 'g'
            UNION
          SELECT '🏃', '⚙️', put(s.grid, s.stack[1], s.stack[2], s.stack[3]), s.input, s.output, s.direction, s.step_length, s.x, s.y,  s.stack[4:]
          WHERE  c = 'p'
            UNION
          SELECT '🏃', '🪡', s.grid, s.input, s.output, s.direction, s.step_length, s.x, s.y, s.stack
          WHERE  c = '"'
            UNION
          SELECT '🏃', '⚙️', s.grid, s.input, s.output, s.direction, s.step_length, s.x, s.y, c :: int || s.stack
          WHERE  c BETWEEN '0' AND '9'
            UNION
          SELECT '🏃', '⚙️', s.grid, s.input, s.output, s.direction, s.step_length, s.x, s.y, s.stack
          WHERE  c != '@' AND c != '>' AND c != 'v' AND c != '<' AND c != '^' AND c != '?' AND c != '#'
          AND    c != '_' AND c != '|' AND c != '+' AND c != '-' AND c != '*' AND c != '/' AND c != '%'
          AND    c != '!' AND c != '`' AND c != '.' AND c != ',' AND c != '&' AND c != '~' AND c != '$'
          AND    c != ':' AND c != '\' AND c != 'g' AND c != 'p' AND c != '"' AND NOT (c BETWEEN '0' AND '9')) AS _
        WHERE  s."execution mode" = '⚙️'
          UNION
        SELECT '🏃', _.*
        FROM   (
          SELECT '⚙️' :: execution_mode, s.grid, s.input, s.output, s.direction, 1, s.x, s.y,s.stack
          WHERE  c = '"'
            UNION
          SELECT '🪡', s.grid, s.input, s.output, s.direction, 1, s.x, s.y, from_str(c) || s.stack
          WHERE  c != '"') AS _
        WHERE s."execution mode" = '🪡'
          UNION
        SELECT s."next execution mode", null, s.grid, s.input, s.output, s.direction, 1, _.*, s.stack
        FROM   (
          SELECT wrap(s.x + s.step_length, w), s.y
          WHERE  s.direction = '🡺'
            UNION
          SELECT s.x, wrap(s.y + s.step_length, h)
          WHERE  s.direction = '🡻'
            UNION
          SELECT wrap(s.x - s.step_length, w), s.y
          WHERE  s.direction = '🡸'
            UNION
          SELECT s.x, wrap(s.y - s.step_length, h)
          WHERE  s.direction = '🡹') AS _
        WHERE s."execution mode" = '🏃') AS "next"
      WHERE  s."execution mode" <> '🏁')
  SELECT s."execution mode" AS mode,
         point(s.x, s.y) AS pos,
         s.direction AS dir,
         s.stack AS stack,
         (SELECT string_agg(to_str(c) || (CASE WHEN i % {width} = 0 THEN E'\n' ELSE '' END), '')
          FROM   unnest(s.grid) WITH ORDINALITY AS _(c,i)) AS grid,
         array_to_string(s.input , '') AS input,
         s.output AS output,
         s.steps AS steps
  FROM   step AS s;
"""

@dataclass(frozen=True)
class Program:
    source: str
    input: list[str] = field(default_factory=list)
    width: int = 80
    height: int = 25
    name: str | None = None


@dataclass(frozen=True)
class Point:
    x: int
    y: int

    class Loader(Loader):
        def load(self, data: bytes) -> Point:
            return Point(*map(int, data.decode("utf-8")[1:-1].split(",", maxsplit=2)))


@dataclass(frozen=True)
class State:
    mode: str
    pos: Point
    dir: str
    stack: list[int]
    grid: str
    input: str
    output: str
    steps: int


@contextmanager
def sec_buffer(enable: bool):
    try:
        if enable:
            print("\033[?1049h\033[22;0;0t\033[0;0H", end="")
        yield
    finally:
        if enable:
            print("\033[?1049l\033[23;0;0t", end="")


class Style(Enum):
    PROGRAM_COUNTER = "36;2"
    BRANCHING = "36;1"
    ARITHMETIC = "34"
    STACK_CONTROL = "33"
    USER_IO = "35"
    GRID_IO = "32"
    STRING_MODE = "31"
    TERMINATE = "31;1"
    NUMBER = "3"
    IGNORE = "2"
    BACKGROUND = ""
    CURSOR = "5;7;90"

    def __radd__(self, other: str) -> str:
        return other + self.value

CLEAR_LINE = "\033[K"

def render_state(program: Program, state: State, colors: dict[Style, str]) -> str:
    out: str = ""
    special: dict[Point, int] = {}
    padded_width = program.width + 1  # include newline!

    for y in range(program.height):
        for x in range(program.width):
            idx = y * padded_width + x
            symbol = state.grid[idx]
            out += "\033[" + colors.get(Style.BACKGROUND, Style.BACKGROUND) + "m"

            if 32 > ord(symbol) or ord(symbol) > 126:
                special[Point(x, y)] = ord(symbol)
                symbol = "▢"

            if x == state.pos.x - 1 and y == state.pos.y - 1:
                out += "\033[" + colors.get(Style.CURSOR, Style.CURSOR) + "m"
            else:
                style: Style

                match symbol:
                    case "<" | ">" | "^" | "v" | "#":
                        style = Style.PROGRAM_COUNTER
                    case "|" | "_" | "?":
                        style = Style.BRANCHING
                    case "+" | "-" | "*" | "/" | "%" | "!" | "`":
                        style = Style.ARITHMETIC
                    case ":" | "\\" | "$":
                        style = Style.STACK_CONTROL
                    case "." | "," | "~" | "&":
                        style = Style.USER_IO
                    case "g" | "p":
                        style = Style.GRID_IO
                    case '"':
                        style = Style.STRING_MODE
                    case "@":
                        style = Style.TERMINATE
                    case _:
                        if symbol.isdigit():
                            style = Style.NUMBER
                        else:
                            style = Style.IGNORE

                out += "\033[" + colors.get(style, style) + "m"

            out += symbol + "\033[0m"
        out += "\n"

    out += f"\033[1mMode:\033[0m {state.mode}\n"
    out += f"\033[1mDirection:\033[0m {state.dir}\n"
    out += f"\033[1mSteps:\033[0m {state.steps}\n"

    out += "\033[1mNon-Printable Values:\033[0m\n"
    if special:
        out += "\n".join(
            f"({pos.x: >2d},{pos.y: >2d}) \033[2m:\033[0m {val:_>4d}" + CLEAR_LINE
            for pos, val in special.items()
        )
    else:
        out += "\033[2m...\033[0m" + CLEAR_LINE

    out += f"\n\033[1mStack\033[0m:{CLEAR_LINE}\n"
    if state.stack:
        out += "\033[2m | \033[0m".join(f"{entry:_>4d}" for entry in state.stack) + CLEAR_LINE
    else:
        out += "\033[2m∅\033[0m" + CLEAR_LINE

    out += f"\n\033[1mOutput\033[0m:{CLEAR_LINE}\n"
    if state.output:
        out += state.output.replace("\n",  CLEAR_LINE + "\n") + CLEAR_LINE
    else:
        out += "\033[2m...\033[0m" + CLEAR_LINE

    return out


app = typer.Typer()


@app.command()
def run(
    program_file: typer.FileBinaryRead,
    authstr: str = typer.Option("", help="Database authentication string."),
    step: bool = typer.Option(False, help="Enable/disable stepper."),
    step_fraction: int = typer.Option(32, help="Fraction of minimal step duration."),
    color_file: typer.FileBinaryRead = typer.Option(None, help="Colors for syntax highlighting.")
):
    colors: dict[Style, str] = (
      {Style[key]: value for key, value in tomllib.load(color_file).items()}
      if color_file is not None else
      {}
    )
    program = Program(**tomllib.load(program_file))
    output: str = ""
    steps: int = 0
    step_time = 1 / step_fraction
    with psycopg.connect(authstr) as conn:
        conn.adapters.register_loader("point", Point.Loader)
        with conn.cursor() as cur:
            code = psycopg.sql.SQL(SETUP)
            cur.execute(code)

        try:
            stdout.write("\033[?25l")
            stdout.flush()
            with conn.cursor(row_factory=psycopg.rows.class_row(State)) as cur, sec_buffer(
                step
            ):
                if step:
                    print(f"\33]0;befunge-sql: {program.name or program_file.name}\a", end='', flush=True)
                skip: int = 0
                last_update: float = time()
                single_step: bool = True
                for state in cur.stream(psycopg.sql.SQL(RUNTIME).format(**asdict(program))):

                    output = state.output
                    steps = state.steps
                    if step:
                        skip = max(0, skip-1)
                        stdout.write("\033[H" + render_state(program, state, colors) + "\n")
                        if skip == 0:
                            if not single_step:
                                stdout.write("\a")
                            stdout.write("\033[2;3mNumber of steps [default: 1]:\033[0m\033[?25h\033[J")
                            stdout.flush()
                            skips = input()
                            stdout.write("\033[?25l")
                            stdout.flush()
                            try:
                                skip = max(1, int(skips))
                            except ValueError:
                                skip = 1
                            single_step = skip == 1
                        else:
                            stdout.write(f"\033[2;3mSteps left: \033[4m{skip}\033[0m\033[J")
                            stdout.flush()
                            current_time = time()
                            if (diff := current_time - last_update) < step_time:
                                sleep(step_time - diff)
                            last_update = time()
        finally:
            stdout.write("\033[?25h")
            stdout.flush()
    if output:
        stdout.flush()
        print(output)
        print(f"({steps=})")


if __name__ == "__main__":
    app()

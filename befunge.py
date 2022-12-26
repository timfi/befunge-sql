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
DROP TYPE IF EXISTS _ret CASCADE;
DROP TYPE IF EXISTS _state CASCADE;
DROP TYPE IF EXISTS direction CASCADE;
DROP TYPE IF EXISTS execution_mode CASCADE;


CREATE TYPE direction AS ENUM
  ('🡺', '🡻', '🡸', '🡹');


CREATE TYPE execution_mode AS ENUM
  ('⚙️', '➡️', '🏁', '🧵');


CREATE TYPE _ret AS (
  pos   point,
  dir   direction,
  stack int[],
  grid  text,
  inp   text,
  outp  text);


CREATE TYPE _state AS (
  ex   execution_mode,
  nex  execution_mode,
  dt   int,
  grid int[][],
  inp  text[],
  outp text,
  d    direction,
  sl   int,
  x    int,
  y    int,
  S    int[]);


CREATE FUNCTION wrap(i int, e int) RETURNS int AS $$
  SELECT 1 + (e + i - 1) % e;
$$ LANGUAGE SQL IMMUTABLE;


-- Function to get a value form a 2D array
CREATE FUNCTION get(grid int[][], _y int, _x int) RETURNS int AS $$
  SELECT grid[y][x]
  FROM   (
    SELECT wrap(coalesce(_y, 0)+1, array_length(grid, 1)),
           wrap(coalesce(_x, 0)+1, array_length(grid, 2))) AS _(y, x);
$$ LANGUAGE SQL IMMUTABLE;


-- Function to set a value in a 2D array
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

    step(ex, nex, dt, grid, inp, outp, d, sl, x, y, S) AS (
      SELECT (
        SELECT ('⚙️', null, 0, grid, {input}, '', '🡺', 1, 1, 1, array[] :: int[]) :: _state
        FROM   preprocess′ AS _(grid)
      ).*
        UNION
      SELECT (
        -- Two step evaultion for each step:
        -- 1. handle current cell
        -- 2. move to next cell
        CASE s.ex
          WHEN '⚙️' THEN
            CASE c
              -- Terminate
              WHEN '@'  THEN ('🏁', null, s.dt, s.grid, s.inp, s.outp, s.d, 1, s.x, s.y, s.S) :: _state
              -- Cursor
              WHEN '>'  THEN ('➡️', '⚙️', s.dt, s.grid, s.inp, s.outp, '🡺', 1, s.x, s.y, s.S) :: _state
              WHEN 'v'  THEN ('➡️', '⚙️', s.dt, s.grid, s.inp, s.outp, '🡻', 1, s.x, s.y, s.S) :: _state
              WHEN '<'  THEN ('➡️', '⚙️', s.dt, s.grid, s.inp, s.outp, '🡸', 1, s.x, s.y, s.S) :: _state
              WHEN '^'  THEN ('➡️', '⚙️', s.dt, s.grid, s.inp, s.outp, '🡹', 1, s.x, s.y, s.S) :: _state
              WHEN '?'  THEN ('➡️', '⚙️', s.dt, s.grid, s.inp, s.outp, (array['🡺', '🡻', '🡸', '🡹'])[round(1 + random() * 3)], 1, s.x, s.y, s.S) :: _state
              WHEN '#'  THEN ('➡️', '⚙️', s.dt, s.grid, s.inp, s.outp, s.d, 2, s.x, s.y, s.S) :: _state
              -- Arithmetic/Compare
              WHEN '+'  THEN ('➡️', '⚙️', s.dt, s.grid, s.inp, s.outp, s.d, 1, s.x, s.y, (coalesce(s.S[2], 0) + coalesce(s.S[1], 0))        || s.S[3:]) :: _state
              WHEN '-'  THEN ('➡️', '⚙️', s.dt, s.grid, s.inp, s.outp, s.d, 1, s.x, s.y, (coalesce(s.S[2], 0) - coalesce(s.S[1], 0))        || s.S[3:]) :: _state
              WHEN '*'  THEN ('➡️', '⚙️', s.dt, s.grid, s.inp, s.outp, s.d, 1, s.x, s.y, (coalesce(s.S[2], 0) * coalesce(s.S[1], 0))        || s.S[3:]) :: _state
              WHEN '/'  THEN ('➡️', '⚙️', s.dt, s.grid, s.inp, s.outp, s.d, 1, s.x, s.y, (coalesce(s.S[2], 0) / coalesce(s.S[1], 0))        || s.S[3:]) :: _state
              WHEN '%'  THEN ('➡️', '⚙️', s.dt, s.grid, s.inp, s.outp, s.d, 1, s.x, s.y, (coalesce(s.S[2], 0) % coalesce(s.S[1], 0))        || s.S[3:]) :: _state
              WHEN '!'  THEN ('➡️', '⚙️', s.dt, s.grid, s.inp, s.outp, s.d, 1, s.x, s.y, (coalesce(s.S[1], 0) = 0                  ) :: int || s.S[2:]) :: _state
              WHEN '`'  THEN ('➡️', '⚙️', s.dt, s.grid, s.inp, s.outp, s.d, 1, s.x, s.y, (coalesce(s.S[2], 0) > coalesce(s.S[1], 0)) :: int || s.S[3:]) :: _state
              -- IO
              WHEN '.'  THEN ('➡️', '⚙️', s.dt, s.grid, s.inp    , s.outp || coalesce(s.S[1], 0) :: text || ' ', s.d, s.sl, s.x, s.y, s.S[2:]                  ) :: _state
              WHEN ','  THEN ('➡️', '⚙️', s.dt, s.grid, s.inp    , s.outp || to_str(coalesce(s.S[1], 0))       , s.d, s.sl, s.x, s.y, s.S[2:]                  ) :: _state
              WHEN '&'  THEN ('➡️', '⚙️', s.dt, s.grid, s.inp[2:], s.outp                                      , s.d, s.sl, s.x, s.y, s.inp[1] :: int    || s.S) :: _state
              WHEN '~'  THEN ('➡️', '⚙️', s.dt, s.grid, s.inp[2:], s.outp                                      , s.d, s.sl, s.x, s.y, from_str(s.inp[1]) || s.S) :: _state
              -- Stack
              WHEN '$'  THEN ('➡️', '⚙️', s.dt, s.grid, s.inp, s.outp, s.d, s.sl, s.x, s.y, s.S[2:]) :: _state
              WHEN ':'  THEN ('➡️', '⚙️', s.dt, s.grid, s.inp, s.outp, s.d, s.sl, s.x, s.y, coalesce(s.S[1], 0) || s.S) :: _state
              WHEN E'\\'THEN ('➡️', '⚙️', s.dt, s.grid, s.inp, s.outp, s.d, s.sl, s.x, s.y, array[coalesce(s.S[2], 0), coalesce(s.S[1], 0)] || s.S[3:]) :: _state
              -- Grid
              WHEN 'g'  THEN ('➡️', '⚙️', s.dt, s.grid, s.inp, s.outp, s.d, s.sl, s.x, s.y, get(s.grid, s.S[1], s.S[2]) || s.S[3:]) :: _state
              WHEN 'p'  THEN ('➡️', '⚙️', s.dt, put(s.grid, s.S[1], s.S[2], s.S[3]), s.inp, s.outp, s.d, s.sl, s.x, s.y,  s.S[4:]) :: _state
              -- Conditional
              WHEN '_'  THEN ('➡️', '⚙️', s.dt, s.grid, s.inp, s.outp, (array['🡸','🡺'])[1+(coalesce(s.S[1], 0)=0)::int], s.sl, s.x, s.y, s.S[2:]) :: _state
              WHEN '|'  THEN ('➡️', '⚙️', s.dt, s.grid, s.inp, s.outp, (array['🡹','🡻'])[1+(coalesce(s.S[1], 0)=0)::int], s.sl, s.x, s.y, s.S[2:]) :: _state
              -- Literal
              WHEN '"'  THEN ('➡️', '🧵', s.dt, s.grid, s.inp, s.outp, s.d, s.sl, s.x, s.y, s.S) :: _state
              ELSE CASE
                WHEN c BETWEEN '0' AND '9'
                        THEN ('➡️', '⚙️', s.dt, s.grid, s.inp, s.outp, s.d, s.sl, s.x, s.y, c :: int || s.S) :: _state
                        ELSE ('➡️', '⚙️', s.dt, s.grid, s.inp, s.outp, s.d, s.sl, s.x, s.y, s.S) :: _state
              END
            END
          WHEN '🧵' THEN
            CASE c
              WHEN '"'  THEN ('➡️', '⚙️', s.dt, s.grid, s.inp, s.outp, s.d, 1, s.x, s.y,                s.S) :: _state
                        ELSE ('➡️', '🧵', s.dt, s.grid, s.inp, s.outp, s.d, 1, s.x, s.y, from_str(c) || s.S) :: _state
            END
          WHEN '➡️' THEN
            CASE s.d
              WHEN '🡺' THEN (s.nex, null, s.dt + 1, s.grid, s.inp, s.outp, s.d, 1, wrap(s.x + s.sl, w), s.y                , s.S) :: _state
              WHEN '🡻' THEN (s.nex, null, s.dt + 1, s.grid, s.inp, s.outp, s.d, 1, s.x                , wrap(s.y + s.sl, h), s.S) :: _state
              WHEN '🡸' THEN (s.nex, null, s.dt + 1, s.grid, s.inp, s.outp, s.d, 1, wrap(s.x - s.sl, w), s.y                , s.S) :: _state
              WHEN '🡹' THEN (s.nex, null, s.dt + 1, s.grid, s.inp, s.outp, s.d, 1, s.x                , wrap(s.y - s.sl, h), s.S) :: _state
            END
        END
      ).*
      FROM    step AS s,
      LATERAL (SELECT to_str(s.grid[s.y][s.x]),
                      {width},
                      {height}) AS _(c,w,h)
      WHERE  s.ex <> '🏁')
  SELECT point(s.x, s.y) AS pos,
         s.d AS dir,
         s.S as stack,
    (SELECT string_agg(to_str(c) || (CASE WHEN i % {width} = 0 THEN E'\n' ELSE '' END), '')
     FROM   unnest(s.grid) WITH ORDINALITY AS _(c,i)) AS grid,
    array_to_string(s.inp , '') AS inp, s.outp AS outp
  FROM   step AS s
  WHERE  s.ex = '➡️'
  LIMIT  {limit};
"""

@dataclass(frozen=True)
class Program:
    source: str
    input: list[str] = field(default_factory=list)
    width: int = 80
    height: int = 25
    limit: int = 100000


@dataclass(frozen=True)
class Point:
    x: int
    y: int

    class Loader(Loader):
        def load(self, data: bytes) -> Point:
            return Point(*map(int, data.decode("utf-8")[1:-1].split(",", maxsplit=2)))


@dataclass(frozen=True)
class State:
    pos: Point
    dir: str
    stack: list[int]
    grid: str
    inp: str
    outp: str


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
                out += "\033[5;7;90m"
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

    out += "\033[1mNon-Printable Values:\033[0m\n"
    if special:
        out += "\n".join(
            f"({pos.x: >2d},{pos.y: >2d}) \033[2m:\033[0m {val:_>4d}" + CLEAR_LINE
            for pos, val in special.items()
        )
    else:
        out += "\033[2m...\033[0m"

    out += f"\n\033[1mStack\033[0m:{CLEAR_LINE}\n"
    if state.stack:
        out += "\033[2m | \033[0m".join(f"{entry:_>4d}" for entry in state.stack) + CLEAR_LINE
    else:
        out += "\033[2m∅\033[0m" + CLEAR_LINE

    out += f"\n\033[1mOutput\033[0m:{CLEAR_LINE}\n"
    if state.outp:
        out += state.outp.replace("\n",  CLEAR_LINE + "\n") + CLEAR_LINE
    else:
        out += "\033[2m...\033[0m" + CLEAR_LINE

    return out


app = typer.Typer()


@app.command()
def run(
    program_file: typer.FileBinaryRead,
    authstr: str = typer.Option("", help="Database authentication string."),
    step: bool = typer.Option(True, help="Enable/disable stepper."),
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
                skip: int = 0
                last_update: float = time()
                for state in cur.stream(psycopg.sql.SQL(RUNTIME).format(**asdict(program))):
                    skip = max(0, skip-1)
                    if step:
                        stdout.write("\033[H" + render_state(program, state, colors) + "\n")
                        if skip == 0:
                            stdout.write("\033[2;3mNumber of steps [default: 1]:\033[0m\033[?25h\033[J")
                            stdout.flush()
                            skips = input()
                            stdout.write("\033[?25l")
                            stdout.flush()
                            try:
                                skip = int(skips)
                            except ValueError:
                                skip = 0
                        else:
                            stdout.write(f"\033[2;3mSteps left: \033[4m{skip}\033[0m\033[J")
                            stdout.flush()
                            current_time = time()
                            if (diff := current_time - last_update) < step_time:
                                sleep(step_time - diff)
                            last_update = time()
                    output = state.outp
            if output:
                print(output)
        finally:
            stdout.write("\033[?25h")
            stdout.flush()


if __name__ == "__main__":
    app()

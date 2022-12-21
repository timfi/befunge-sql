DROP FUNCTION IF EXISTS befunge(text, text[], int, int, int);
DROP FUNCTION IF EXISTS put(int[][], int, int, int);
DROP FUNCTION IF EXISTS get(int[][], int, int);
DROP FUNCTION IF EXISTS to_str(int);
DROP FUNCTION IF EXISTS from_str(text);
DROP FUNCTION IF EXISTS wrap(int, int);
DROP TYPE IF EXISTS _ret;
DROP TYPE IF EXISTS _state;
DROP TYPE IF EXISTS direction;
DROP TYPE IF EXISTS execution_mode;


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


CREATE FUNCTION befunge(src text, inp text[] = '{}'::text[], width int = 80, height int = 25, lim int = 100000) RETURNS SETOF _ret AS $$
  WITH RECURSIVE
    -- preprocess takes the input program/playfield string and…
    --  1. ensures the correct dimensions
    --  2. transform it in to a 2D array
    preprocess(grid) AS (
      SELECT (
        (array_agg(
          string_to_array(
            rpad(l, befunge.width, ' '), NULL)
          )
        )[1:befunge.height][1:befunge.width])
      FROM regexp_split_to_table(
        befunge.src ||
        repeat(
          repeat(' ', befunge.width) || E'\n',
          befunge.height),
        '\n') WITH ORDINALITY AS l),

    preprocess′(grid) AS (
      SELECT  array_agg(r ORDER BY y)
      FROM    preprocess AS _(grid),
      LATERAL (
        SELECT y, array_agg(from_str(grid[y][x]) ORDER BY x)
        FROM   generate_series(1,befunge.height) AS y,
               generate_series(1,befunge.width)  AS x
        GROUP BY y) AS __(y, r)),

    step(ex, nex, dt, grid, inp, outp, d, sl, x, y, S) AS (
      SELECT (
        SELECT ('⚙️', null, 0, grid, befunge.inp, '', '🡺', 1, 1, 1, array[] :: int[]) :: _state
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
                      befunge.width,
                      befunge.height) AS _(c,w,h)
      WHERE  s.ex <> '🏁')
  SELECT (
    point(s.x, s.y), s.d, s.S,
    (SELECT string_agg(to_str(c) || (CASE WHEN i % befunge.width = 0 THEN E'\n' ELSE '' END), '')
     FROM   unnest(s.grid) WITH ORDINALITY AS _(c,i)),
    array_to_string(s.inp , ''), s.outp) :: _ret
  FROM   step AS s
  WHERE  s.ex = '➡️'
  LIMIT  befunge.lim;
$$ LANGUAGE SQL IMMUTABLE;

DROP FUNCTION IF EXISTS befunge(text, text[], int, int) CASCADE;
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
  ('⚙️', '🪡', '🏁');

CREATE FUNCTION wrap(i int, e int) RETURNS int AS $$
  SELECT 1 + (e + i - 1) % e;
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION get(grid int[][], _y int, _x int) RETURNS int AS $$
  SELECT grid[y][x]
  FROM   (
          SELECT wrap(
                  coalesce(_y, 0) + 1,
                  array_length(grid, 1)
                 ),
                 wrap(
                  coalesce(_x, 0) + 1,
                  array_length(grid, 2)
                 )
         ) AS _(y, x);
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION put(grid int[][], _y int, _x int, _v int) RETURNS int[][] AS $$
  SELECT array_agg(r ORDER BY Y′)
  FROM   (
          SELECT wrap(
                  coalesce(_y, 0) + 1,
                  array_length(grid, 1)
                 ),
                 wrap(
                  coalesce(_x, 0) + 1,
                  array_length(grid, 2)
                 ),
                 coalesce(_v, 0)
         ) AS _(y, x, v),
        LATERAL (
            SELECT Y′, array_agg(CASE (X′,Y′) WHEN (x,y) THEN v ELSE grid[Y′][X′] END ORDER BY X′)
            FROM   generate_series(1,array_length(grid,1)) AS Y′,
                   generate_series(1,array_length(grid,2)) AS X′
            GROUP BY Y′
        ) AS __(Y′, r);
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

CREATE FUNCTION befunge(source text, input text[] = '{}'::text[], width int = 80, height int = 25) RETURNS text AS $$
  WITH RECURSIVE
    step(execution_mode, grid, input, output, stack, direction, x, y) AS (
      SELECT '⚙️' :: execution_mode,
             grid,
             befunge.input,
             '' AS output,
             array[] :: int[] AS stack,
             '🡺' :: direction,
             1,
             1
      FROM   (SELECT  array_agg(r ORDER BY y)
              FROM    LATERAL (
                        SELECT (array_agg(string_to_array(rpad(l, befunge.width, ' '), NULL)))[1:befunge.height][1:befunge.width]
                        FROM   regexp_split_to_table(
                                befunge.source ||
                                repeat(repeat(' ', befunge.width) || E'\n',befunge.height),
                                '\n'
                               ) WITH ORDINALITY AS l
                      ) AS _(grid),
                      LATERAL (
                        SELECT y, array_agg(from_str(grid[y][x]) ORDER BY x)
                        FROM   generate_series(1,befunge.height) AS y,
                               generate_series(1,befunge.width)  AS x
                        GROUP BY y
                      ) AS __(y, r)
             ) AS _(grid)
        UNION ALL -- recursive UNION!
      SELECT  next.execution_mode,
              next.grid,
              next.input,
              next.output,
              next.stack,
              next.direction,
              move.x,
              move.y
      FROM    step AS current,
              LATERAL (SELECT to_str(current.grid[current.y][current.x])) AS _(symbol),
              -- Process
              LATERAL (
                -- Normal Mode
                SELECT _.*
                FROM   (
                        -- Termination
                        SELECT '🏁' :: execution_mode, current.grid, current.input, current.output, current.stack, current.direction, 0
                        WHERE  symbol = '@'
                          UNION ALL

                        -- Program Counter Control
                        SELECT '⚙️', current.grid, current.input, current.output, current.stack, _.*
                        FROM   (
                                SELECT current.direction, 2
                                WHERE  symbol = '#'
                                  UNION ALL
                                SELECT '🡺', 1
                                WHERE  symbol = '>'
                                  UNION ALL
                                SELECT '🡻', 1
                                WHERE  symbol = 'v'
                                  UNION ALL
                                SELECT '🡸', 1
                                WHERE  symbol = '<'
                                  UNION ALL
                                SELECT '🡹', 1
                                WHERE  symbol = '^'
                               ) AS _(direction, step_length)
                          UNION ALL

                        -- Branching
                        SELECT '⚙️', current.grid, current.input, current.output, _.*, 1
                        FROM   (
                                SELECT current.stack, (array['🡺', '🡻', '🡸', '🡹'])[round(1 + random() * 3)] :: direction
                                WHERE  symbol = '?'
                                  UNION ALL
                                SELECT current.stack[2:], (array['🡸','🡺'])[1+(coalesce(current.stack[1], 0)=0)::int] :: direction
                                WHERE  symbol = '_'
                                  UNION ALL
                                SELECT current.stack[2:], (array['🡹','🡻'])[1+(coalesce(current.stack[1], 0)=0)::int] :: direction
                                WHERE  symbol = '|'
                               ) AS _(stack, direction)
                          UNION ALL

                        -- Arithmetic
                        SELECT '⚙️', current.grid, current.input, current.output, _.*, current.direction, 1
                        FROM   (
                                SELECT (coalesce(current.stack[2], 0) + coalesce(current.stack[1], 0)) || current.stack[3:]
                                WHERE  symbol = '+'
                                  UNION ALL
                                SELECT (coalesce(current.stack[2], 0) - coalesce(current.stack[1], 0)) || current.stack[3:]
                                WHERE  symbol = '-'
                                  UNION ALL
                                SELECT (coalesce(current.stack[2], 0) * coalesce(current.stack[1], 0)) || current.stack[3:]
                                WHERE  symbol = '*'
                                  UNION ALL
                                SELECT (coalesce(current.stack[2], 0) / coalesce(current.stack[1], 0)) || current.stack[3:]
                                WHERE  symbol = '/'
                                  UNION ALL
                                SELECT (coalesce(current.stack[2], 0) % coalesce(current.stack[1], 0)) || current.stack[3:]
                                WHERE  symbol = '%'
                                  UNION ALL
                                SELECT (coalesce(current.stack[1], 0) = 0) :: int || current.stack[2:]
                                WHERE  symbol = '!'
                                  UNION ALL
                                SELECT (coalesce(current.stack[2], 0) > coalesce(current.stack[1], 0)) :: int || current.stack[3:]
                                WHERE  symbol = '`'
                               ) AS _(stack)
                          UNION ALL

                        -- User IO
                        SELECT '⚙️', current.grid, _.*, current.direction, 1
                        FROM   (
                                SELECT current.input, current.output || coalesce(current.stack[1], 0) :: text || ' ', current.stack[2:]
                                WHERE  symbol = '.'
                                  UNION ALL
                                SELECT current.input, current.output || to_str(coalesce(current.stack[1], 0)), current.stack[2:]
                                WHERE  symbol = ','
                                  UNION ALL
                                SELECT current.input[2:], current.output, current.input[1] :: int || current.stack
                                WHERE  symbol = '&'
                                  UNION ALL
                                SELECT current.input[2:], current.output, from_str(current.input[1]) || current.stack
                                WHERE  symbol = '~'
                               ) AS _(input, output, stack)
                          UNION ALL

                        -- Stack Control
                        SELECT '⚙️', current.grid, current.input, current.output, _.*, current.direction, 1
                        FROM   (
                                SELECT current.stack[2:]
                                WHERE  symbol = '$'
                                  UNION ALL
                                SELECT coalesce(current.stack[1], 0) || current.stack
                                WHERE  symbol = ':'
                                  UNION ALL
                                SELECT array[coalesce(current.stack[2], 0), coalesce(current.stack[1], 0)] || current.stack[3:]
                                WHERE  symbol = '\'
                               ) AS _(stack)
                          UNION ALL

                        -- Grid IO
                        SELECT '⚙️', _.grid, current.input, current.output, _.stack, current.direction, 1
                        FROM   (
                                SELECT current.grid, get(current.grid, current.stack[1], current.stack[2]) || current.stack[3:]
                                WHERE  symbol = 'g'
                                  UNION ALL
                                SELECT put(current.grid, current.stack[1], current.stack[2], current.stack[3]), current.stack[4:]
                                WHERE  symbol = 'p'
                               ) AS _(grid, stack)
                          UNION ALL

                        -- String Mode Toggle
                        SELECT '🪡', current.grid, current.input, current.output, current.stack, current.direction, 1
                        WHERE  symbol = '"'
                          UNION ALL

                        -- Numbers
                        SELECT '⚙️', current.grid, current.input, current.output, symbol :: int || current.stack, current.direction, 1
                        WHERE  symbol BETWEEN '0' AND '9'
                          UNION ALL

                        -- Comments
                        SELECT '⚙️', current.grid, current.input, current.output, current.stack, current.direction, 1
                        WHERE  symbol != '@'
                        AND    symbol != '>'
                        AND    symbol != 'v'
                        AND    symbol != '<'
                        AND    symbol != '^'
                        AND    symbol != '?'
                        AND    symbol != '#'
                        AND    symbol != '_'
                        AND    symbol != '|'
                        AND    symbol != '+'
                        AND    symbol != '-'
                        AND    symbol != '*'
                        AND    symbol != '/'
                        AND    symbol != '%'
                        AND    symbol != '!'
                        AND    symbol != '`'
                        AND    symbol != '.'
                        AND    symbol != ','
                        AND    symbol != '&'
                        AND    symbol != '~'
                        AND    symbol != '$'
                        AND    symbol != ':'
                        AND    symbol != '\'
                        AND    symbol != 'g'
                        AND    symbol != 'p'
                        AND    symbol != '"'
                        AND    NOT (symbol BETWEEN '0' AND '9')
                       ) AS _(execution_mode, grid, input, output, stack, direction, step_length)
                WHERE  current.execution_mode = '⚙️'
                  UNION ALL

                -- String Mode
                SELECT _.execution_mode, current.grid, current.input, current.output, _.stack, current.direction, 1
                FROM   (
                        SELECT '⚙️' :: execution_mode, current.stack
                        WHERE  symbol = '"'
                          UNION ALL
                        SELECT '🪡', from_str(symbol) || current.stack
                        WHERE  symbol != '"'
                       ) AS _(execution_mode, stack)
                WHERE current.execution_mode = '🪡'
              ) AS "next"(execution_mode, grid, input, output, stack, direction, step_length),

              -- Move
              LATERAL (
                SELECT wrap(current.x + next.step_length, befunge.width), current.y
                WHERE  next.direction = '🡺'
                  UNION ALL
                SELECT current.x, wrap(current.y + next.step_length, befunge.height)
                WHERE  next.direction = '🡻'
                  UNION ALL
                SELECT wrap(current.x - next.step_length, befunge.width), current.y
                WHERE  next.direction = '🡸'
                  UNION ALL
                SELECT current.x, wrap(current.y - next.step_length, befunge.height)
                WHERE  next.direction = '🡹'
              ) AS move(x, y)
      WHERE  current.execution_mode <> '🏁'
    )
  SELECT output
  FROM   step
  WHERE  execution_mode = '🏁';
$$ LANGUAGE SQL IMMUTABLE;

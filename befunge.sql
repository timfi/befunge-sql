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
  outp text[],
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
  SELECT CASE c
    WHEN 0 THEN '␀'
           ELSE chr(c)
  END;
$$ LANGUAGE SQL IMMUTABLE;


CREATE FUNCTION from_str(c text) RETURNS int AS $$
  SELECT CASE c
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
        SELECT ('⚙️', null, 0, grid, befunge.inp, array[] :: text[], '🡺', 1, 1, 1, array[] :: int[]) :: _state
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
              WHEN '.'  THEN ('➡️', '⚙️', s.dt, s.grid, s.inp    , s.outp || coalesce(s.S[1], 0) :: text, s.d, s.sl, s.x, s.y, s.S[2:]               ) :: _state
              WHEN ','  THEN ('➡️', '⚙️', s.dt, s.grid, s.inp    , s.outp || to_str(coalesce(s.S[1], 0)), s.d, s.sl, s.x, s.y, s.S[2:]               ) :: _state
              WHEN '&'  THEN ('➡️', '⚙️', s.dt, s.grid, s.inp[2:], s.outp                               , s.d, s.sl, s.x, s.y,    s.inp[1] :: int || s.S) :: _state
              WHEN '~'  THEN ('➡️', '⚙️', s.dt, s.grid, s.inp[2:], s.outp                               , s.d, s.sl, s.x, s.y, from_str(s.inp[1]) || s.S) :: _state
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
    array_to_string(s.inp , ''),
    array_to_string(s.outp, '')) :: _ret
  FROM   step AS s
  WHERE  s.ex = '➡️'
  LIMIT  befunge.lim;
$$ LANGUAGE SQL IMMUTABLE;


-- Simple calculation
-- SELECT * FROM befunge(
--   '43+.@',
--   width := 5, height := 1);

-- Simple calculation, but in 2D!
-- SELECT * FROM befunge(
--   E'v >.v\n' ||
--   E'4 + @\n' ||
--   E'>3^  ',
--   width := 5, height := 3);

-- Echo
-- SELECT * FROM befunge(
--   '~:1+!#@_,',
--   inp := string_to_array('hello there!'),
--   width := 9, height := 1);

-- To Upper
-- SELECT * FROM befunge(
--   E'v,         <         <       <\n' ||
--   E'>~:"a"1-`!#^_:"z"1+`#^_"aA"--^',
--   inp := string_to_array('lowercase'),
--   width := 30, height := 2);

-- Factorial
-- SELECT * FROM befunge(
--   E'&>:1-:v v *_$.@\n' ||
--    ' ^    _$>\:^     ',
--   inp := array['12'],
--   width := 15, height := 2);

-- ASCII to Hex
-- SELECT * FROM befunge(
--   E'~:25*-#v_@      >  >," ",    \n' ||
--   E'v      < >25*-"A"+v^+"A"-*52<\n' ||
--   E'>:82*/:9`|      " >,:82*%:9`|\n' ||
--   E'         >"0"+ #^ ^#    +"0"<',
--   inp := string_to_array('hello world!'),
--   width := 60, height := 4);

-- rot13
-- SELECT * FROM befunge(
--   'v,-9-4_v#!`"z":<                ' || E'\n' ||
--   '>~::1+! #@_"m"`|                ' || E'\n' ||
--   '^<$,   <       :<   -6-7<       ' || E'\n' ||
--   '^,<+5+8<_v#`"`"<                ' || E'\n' ||
--   '#^_    ^ >:"Z"`#^_:"M"`#^_:"@"`!',
--   inp := string_to_array('hello world'),
--   width := 32, height := 5);

-- Bin to Dec
-- SELECT * FROM befunge(
-- --    vvvvvvvv 8-Bit Binary Number input here!
--   'v ;11101010;                                          ' || E'\n' ||
--   '>>>>>>>>>>>>>>>a0g68*-90g68*-2*+80g68*-4*+70g68*-8*+v ' || E'\n' ||
--   '@.+***288-*86g03+**88-*86g04+**84-*86g05+**44-*86g06< ',
--   width := 54, height := 3);

-- Primes
-- SELECT * FROM befunge(
--   '222p882**1+11p>133p                   >33g1+33p   22g33g- v>22g33g%#v_v' || E'\n' ||
--   ' o                                                        >|           ' || E'\n' ||
--   '  2                              v,,,, ,,,,,.g22" is prime"<           ' || E'\n' ||
--   '   1                             >   v^                             <  ' || E'\n' ||
--   '              ^_@#-g11g22p22+1g22,*25<,,,,,,,,,,,,,.g22" is not prime"<');

-- Sin waves
-- SELECT * FROM befunge(
--   '100p110p 88*:*8*2- v                                       v       <    ' || E'\n' ||
--   '>                  > : 88*:*8* / 00g* 25*::*8*\4*+ / 58* + >".",1-:|    ' || E'\n' ||
--   '  v                                                      ,*52,"*"$ <    ' || E'\n' ||
--   '  > :: 88*:*8* % 10g* 88*:*8*88*-1+ * \ 88*:*8* / 00g* 25*::**2*3- * - v' || E'\n' ||
--   '  v                                                        / -2*8*:*88 <' || E'\n' ||
--   '  > \: 88*:*8* / 00g* 88*:*8*88*-1+ * \ 88*:*8* % 10g* 25*::**2*3- * + v' || E'\n' ||
--   '  v                                                        / -2*8*:*88 <' || E'\n' ||
--   '          > 100p       v         > 110p       v                         ' || E'\n' ||
--   '  > : 0 ` |            > \ : 0 ` |            > \ 88*:*8* * + v         ' || E'\n' ||
--   '          > 01-00p 0\- ^         > 01-10p 0\- ^                         ' || E'\n' ||
--   '^                                                             <         ',
--   lim := 10000);

-- Game of Life
-- SELECT * FROM befunge(
--   'v>>31g> ::51gg:2v++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++' || E'\n' ||
--   '9p BXY|-+<v3*89<%+ *                                                      *   +' || E'\n' ||
--   '21 >98 *7^>+\-0|<+ *                                                     *    +' || E'\n' ||
--   '*5 ^:+ 1pg15\,:< + *                                                     ***  +' || E'\n' ||
--   '10^  <>$25*,51g1v+                                                            +' || E'\n' ||
--   '-^ p<| -*46p15:+<+                                                            +' || E'\n' ||
--   '> 31^> 151p>92*4v+                                                            +' || E'\n' ||
--   ' ^_ ".",   ^ vp1<+                                                            +' || E'\n' ||
--   '>v >41p      >0 v+                                                            +' || E'\n' ||
--   ':5! vg-1g15-1g14<+                                                            +' || E'\n' ||
--   '+1-+>+41g1-51gg+v+                                                            +' || E'\n' ||
--   '1p-1vg+1g15-1g14<+                                                            +' || E'\n' ||
--   'g61g>+41g51g1-g+v+                                                            +' || E'\n' ||
--   '14*1v4+g+1g15g14<+                           * *                              +' || E'\n' ||
--   '5>^4>1g1+51g1-g+v+                           * *                              +' || E'\n' ||
--   '^ _^v4+gg15+1g14<+                           ***                              +' || E'\n' ||
--   '>v! >1g1+51g1+g+v+                                                            +' || E'\n' ||
--   'g8-v14/*25-*4*88<+                                                            +' || E'\n' ||
--   '19+>g51gg" "- v  +                                                            +' || E'\n' ||
--   '4*5  v<   v-2:_3v+                                                            +' || E'\n' ||
--   ' >^   |!-3_$  v<-+                                                            +' || E'\n' ||
--   '^    < <      <|<+                                                         ***+' || E'\n' ||
--   '>g51gp ^ >51gp^>v+                                                            +' || E'\n' ||
--   '^14"+"<  ^g14"!"<++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++',
--   lim := 10000);


-- Mycology test suite (befunge-93 portion)
SELECT * FROM befunge(
  E'0#@>. 1#@v>#@,55+"skrow , :DOOG",,,,,,,,,,,,,,1#v:$v>"pud t\'nseod : DAB",,,,,,,v\n' ||
  E'v"@.4"@#<.+@,,,,,,,,,,,,,,,,"BAD: : reflects"+55<v _^          @,+55,,,,,,,,,,,<\n' ||
  E'>$#v5#.< #5   >:#,_$v#"GGGOOD: : duplicates"+730$<>"OOG">:#,_22#v-#v_19+"0 = 2"v\n' ||
  E'>3.#@$ .^@5v"ro"+820_28+"orez spop kcats ytpme :D"^v"-2 != 0"+550  <v"GOOD: 2-"<\n' ||
  E'^ 0@# 4.2< >"eznon spop kcats ytpme :DAB"v  "BAD: - reflects"+55<   >:#,_v  >v  \n' ||
  E'v.6_5.@>7.^>+"stcelfer \\">" :DAB">:#,_@#:< "BAD: 2"<   v"GOOD: | works"+<>#v|>0v\n' ||
  E'>80#@+#^_@ ^55>#0<  >:#,_ v#:"GOOD: 8*0 = 0"+5<v ># $< >:#,_v           ^550<  5\n' ||
  E'v"D: # < jumps into <"+5<^     "8*0 != 0"+55< 5v"BAD: | goes the wrong way"5#+5<\n' ||
  E'>"OOG">:#,_12#^\\1-#v_8+v5^ "* reflects"+550<^_^>:#,_@#:<"BAD: | reflects"+5<    \n' ||
  E'v   "GOOD: \\ swaps"0#  <^ ># <46+"< rev"v>#^*^>" :DAB">:#,_@>#v!1-#v_55+"1 = "v \n' ||
  E'>:#,_1#v`v >"r `"  5> $  ^ "# < jumps o"<^8<> ^"! reflects"+64<v550<  v"D: 0!"< \n' ||
  E'v68+55<>055+"stce"v>5+"paws t\'nseod "v>:#,_^^"7! != 0"+_v#!773<>+"1 =! !0 :DAB"v\n' ||
  E'*   v_^#$< ^"23fl"]>^   $<0  "BAD: \\"<^  "GOOD: 7! = 0"+<>:#,_^#"GGOO"<>:#,_@#:<\n' ||
  E'    >055+"0 =! `10"^>1\\`1-#v_55+"1 = `01 :DOOG">:#,_900#vp#vg9-v       ^"BAD: "<\n' ||
  E'>" = `10 :DOOG">:#,_^      >55+"1 =! `01 :DAB"  0^      >055+    "stcelfer p"  ^\n' ||
  E'v"difies space"+># 5# <  ^00      "900pg doesn\'t get 9"+55<>5+0 \\"stcelfer g"  ^\n' ||
  E'>"om p :DOOG"vv5 5p:+88"^"_,#! #:<"GOOD: 900pg gets 9"+55_^#!  <                \n' ||
  E'     $_,#! #:<>+"ecaps yfidom t\'nseod p :DAB">:#,_@v"GOOD: wraparound works"v###\n' ||
  E'#                          " column 80",:+55_,#! #:<v"p skips column 80"+550< vv\n' ||
  E'v         "GOOD: Funge-93 spaces"+55<               >"arw :DAB">:#,_@ v"skips"< \n' ||
  E'v "BAD: SGML spaces in Funge-93"+55_^#$\\`"  !"_,#! #:<"UNDEF: edge # "<  "hits"<\n' ||
  E'>:#,_55+"...gnittiuQ"55+".enod si etius tset ygolocyM eht fo noisrev 39-egnufe"v\n' ||
  E'@,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,"The B"<');
\i ../befunge.sql

SELECT * FROM befunge(
--    vvvvvvvv 8-Bit Binary Number input here!
  'v ;11101010;                                          ' || E'\n' ||
  '>>>>>>>>>>>>>>>a0g68*-90g68*-2*+80g68*-4*+70g68*-8*+v ' || E'\n' ||
  '@.+***288-*86g03+**88-*86g04+**84-*86g05+**44-*86g06< ',
  width := 54, height := 3
);

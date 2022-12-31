\i ../befunge.sql

SELECT * FROM befunge(
  '~:!#@_,',
  width := 9, height := 1
  input := string_to_array('hello there!', NULL),
);

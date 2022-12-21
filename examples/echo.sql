\i ../befunge.sql

SELECT * FROM befunge(
  '~:!#@_,',
  inp := string_to_array('hello there!', NULL),
  width := 9, height := 1);

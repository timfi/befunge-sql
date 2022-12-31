\i ../befunge.sql

SELECT * FROM befunge(
  E'v,              <         <       <\n' ||
  E'>~:!#@_:"a"1-`!#^_:"z"1+`#^_"aA"--^',
  width := 35, height := 2
  input := string_to_array('lowercase', NULL),
);

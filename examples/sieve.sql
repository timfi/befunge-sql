\i ../befunge.sql

SELECT * FROM befunge(
  '2>:3g" "-!v\  g30          <                                                    ' || E'\n' ||
  ' |!`"O":+1_:.:03p>03g+:"O"`|                                                    ' || E'\n' ||
  ' @               ^  p3\" ":<                                                    ' || E'\n' ||
  '2 234567890123456789012345678901234567890123456789012345678901234567890123456789',
  height := 4, width := 80
);

\i ../befunge.sql

SELECT * FROM befunge(
  ' &>:1-:v v *_$.@' || E'\n' ||
  '  ^    _$>\:^   ',
  width := 15, height := 2,
  input := ARRAY['6']
);

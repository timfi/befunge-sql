\i ../befunge.sql

SELECT * FROM befunge(
  ' 211p&01p>121p          >21g1+21p 11g21g-v>11g21g%#v_v' || E'\n' ||
  '                                         >|           ' || E'\n' ||
  '                  v,,,,, ,,,.g11"is prime"<           ' || E'\n' ||
  ' >       ^        >   v ^                          <  ' || E'\n' ||
  ' ^_@#-g10p11:+1g11,*25<,,,,,,,,,,,,.g11"is not prime"<',
  inp := ARRAY['25']);

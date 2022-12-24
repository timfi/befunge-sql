\i ../befunge.sql

SELECT * FROM befunge(
  '222p882**1+11p>133p                    >33g1+33p 22g33g-v>22g33g%#v_v' || E'\n' ||
  ' o                                                      >|           ' || E'\n' ||
  '  2                      v,,,,,,,,,,,,. g22"is ___ prime"<           ' || E'\n' ||
  '   1                     >           v ^                          <  ' || E'\n' ||
  '              ^_@#-g11g22p22+1g22,*25<,,,,,,,,,,,,.g22"is not prime"<',
  lim := 10000);

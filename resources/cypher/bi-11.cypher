PROFILE
MATCH
    (c: Person)-[_a: knows]->(a: Person),
    (c)-[_b: isLocatedIn]->(city_c: City),
    (c)-[_c: knows]->(b: Person),
    (a)-[_d: knows]->(b),
    (a)-[_e: isLocatedIn]->(city_a: City),
    (b)-[_f: isLocatedIn]->(city_b: City),
    (city_a)-[_g: isPartOf]->(country: Country),
    (city_b)-[_h: isPartOf]->(country),
    (city_c)-[_i: isPartOf]->(country)
WHERE
    country.name = 'China' AND
    _a.creationDate >= 1284505856158 AND
    _c.creationDate >= 1282382587409 AND
    _d.creationDate >= 1281681940915
RETURN 
    elementId(c) as c,
    elementId(a) as a,
    elementId(b) as b,
    elementId(city_c) as city_c,
    elementId(city_a) as city_a,
    elementId(city_b) as city_b,
    elementId(country) as country,
    elementId(_a) as _a,
    elementId(_b) as _b,
    elementId(_c) as _c,
    elementId(_d) as _d,
    elementId(_e) as _e,
    elementId(_f) as _f,
    elementId(_g) as _g,
    elementId(_h) as _h,
    elementId(_i) as _i
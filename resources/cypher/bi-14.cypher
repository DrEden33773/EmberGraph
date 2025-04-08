PROFILE
MATCH
    (person1: Person)-[a: isLocatedIn]->(city1: City),
    (person2: Person)-[b: isLocatedIn]->(city2: City),
    (person1)-[c: knows]->(person2),
    (city1)-[d: isPartOf]->(country1: Country),
    (city2)-[e: isPartOf]->(country2: Country)
WHERE
    country1.name = 'Afghanistan' and
    country2.name = 'India'
ORDER BY
    country1.name ASC, country2.name ASC
RETURN
    person1,
    person2,
    city1,
    city2,
    country1,
    country2,
    a,
    b,
    c,
    d,
    e
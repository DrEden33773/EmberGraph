match
    (person1: Person)-[a: isLocatedIn]->(city1: City),
    (person2: Person)-[b: isLocatedIn]->(city2: City),
    (person1)-[c: knows]->(person2),
    (city1)-[d: isPartOf]->(country1: Country),
    (city2)-[e: isPartOf]->(country2: Country)
where
    country1.name = 'Afghanistan' and
    country2.name = 'India'
order by
    country1.name asc, country2.name asc
return
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
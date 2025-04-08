PROFILE
MATCH
    (post: Post)-[a: hasCreator]->(zombie: Person),
    (zombie)-[b: isLocatedIn]->(city: City),
    (city)-[c: isPartOf]->(country: Country),
    (likerPerson: Person)-[d: likes]->(post)
WHERE
    country.name = 'China' AND
    post.creationDate < 1266275323907 AND
    zombie.creationDate < 1262778975430 AND
    likerPerson.creationDate < 1281656675810
RETURN
    post,
    zombie,
    city,
    country,
    likerPerson,
    a,
    b,
    c,
    d
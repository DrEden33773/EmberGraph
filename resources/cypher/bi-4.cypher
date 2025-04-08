PROFILE
MATCH
    (forum: Forum)-[a: hasMember]->(member: Person),
    (member)-[b: isLocatedIn]->(city: City),
    (city)-[c: isPartOf]->(country: Country)
WHERE
    forum.creationDate > 1290494980289
RETURN
    forum, member, city, country, a, b, c
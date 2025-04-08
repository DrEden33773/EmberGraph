PROFILE
MATCH
    (person1: Person)-[a: knows]->(person2: Person),
    (person1)-[b: isLocatedIn]->(city1: City),
    (person2)-[c: isLocatedIn]->(city2: City),
    (comment: Comment)-[d: hasCreator]->(person1),
    (message: Comment)-[e: hasCreator]->(person2),
    (comment)-[f: replyOf]->(message)
WHERE
    city1.id = 1342
    and city2.id = 1127
RETURN
    person1,
    person2,
    city1,
    city2,
    comment,
    message,
    a,
    b,
    c,
    d,
    e,
    f
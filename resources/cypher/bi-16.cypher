MATCH
    (post: Post)-[a: hasCreator]->(person1: Person),
    (post)-[b: hasTag]->(tag: Tag),
    (person1)-[c: knows]->(person2: Person)
WHERE
    post.creationDate = 1268422500645
    and tag.name = 'Best_Thing_I_Never_Had'
RETURN
    post,
    person1,
    person2,
    tag,
    a,
    b,
    c
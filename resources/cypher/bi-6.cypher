MATCH
    (person2: Person)-[a: likes]->(post1: Post),
    (post1)-[b: hasTag]->(tag: Tag),
    (post1)-[c: hasCreator]->(person1: Person),
    (person2)<-[d: hasCreator]-(post2: Post),
    (post2)<-[e: likes]-(person3: Person)
WHERE
    tag.name = 'The_Mouse_and_the_Mask'
RETURN person2, post1, tag, person1, post2, person3, a, b, c, d, e
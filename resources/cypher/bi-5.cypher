MATCH
    (post: Post)-[a: hasCreator]->(person: Person),
    (post)-[b: hasTag]->(tag: Tag),
    (liker: Person)-[c: likes]->(post),
    (comment: Comment)-[d: replyOf]->(post)
WHERE
    tag.name = 'Fridtjof_Nansen'
RETURN
    post, person, tag, liker, comment, a, b, c, d
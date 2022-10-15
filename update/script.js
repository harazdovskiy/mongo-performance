db.getCollection("63mil-collection").aggregate([
    { $group: { _id: '$language', count: { $sum: 1 } } },
    { $sort: { 'count': -1 } }
])

db = db.getSiblingDB('hespress_db');

// Create comments collection
db.createCollection('comments');

// Create any necessary indexes
db.comments.createIndex({ "id": 1 }, { unique: true });
db.comments.createIndex({ "created_at": 1 }); 
// middlewares/authMiddleware.js
const jwt = require("jsonwebtoken");

const AuthenticateToken = async (req, res, next) => {
  // Check if the token is in the 'Authorization' header
  const authHeader = req.headers["authorization"];
  let token;
  if (authHeader && authHeader.startsWith("Bearer ")) {
    token = authHeader.split(" ")[1];
  }

  if (!token) {
    return res
      .status(401)
      .json({ message: "Access denied. No token provided." });
  }

  try {
    // Verify the token with the secret
    const verified = jwt.verify(token, process.env.JWT_SECRET);

    if (typeof verified === "string" || !verified.email) {
      return res.status(401).json({ message: "Token invalid or expired" });
    }

    // Set the user information in the request
    req.user = { email: verified.email, roles: verified.roles };

    // Proceed to the next middleware or route handler
    next();
  } catch (error) {
    console.error("JWT verification failed:", error.message);
    res.status(401).json({ message: "Invalid token" });
  }
};

module.exports = AuthenticateToken;

# 📚 Cannabis Data Platform API Documentation

## Authentication

All API requests require authentication using JWT Bearer tokens.

### Get Access Token

```http
POST /auth/login
Content-Type: application/json

{
  "username": "your_username",
  "password": "your_password"
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "access_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...",
    "token_type": "bearer",
    "expires_in": 3600,
    "refresh_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9..."
  }
}
```

### Using Access Token

Include the token in the Authorization header:

```http
Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...
```

## Base URL

```
Production: https://api.weedhounds.com
Staging: https://staging-api.weedhounds.com
Local: http://localhost:8000
```

## Response Format

All API responses follow this standardized format:

```json
{
  "success": boolean,
  "data": object | array | null,
  "meta": {
    "timestamp": "2025-11-06T10:30:00Z",
    "request_id": "req_abc123",
    "processing_time_ms": 45,
    "version": "2.0.0",
    "rate_limit": {
      "limit": 1000,
      "remaining": 995,
      "reset": 1699264200
    }
  },
  "errors": array
}
```

## 🌿 Strain Endpoints

### Get All Strains

```http
GET /api/strains
```

**Query Parameters:**
- `limit` (integer, optional): Number of results to return (default: 100, max: 1000)
- `offset` (integer, optional): Number of results to skip (default: 0)
- `state` (string, optional): Filter by state abbreviation (e.g., "CA", "CO")
- `type` (string, optional): Filter by strain type ("indica", "sativa", "hybrid")
- `thc_min` (float, optional): Minimum THC percentage
- `thc_max` (float, optional): Maximum THC percentage
- `cbd_min` (float, optional): Minimum CBD percentage
- `cbd_max` (float, optional): Maximum CBD percentage
- `sort` (string, optional): Sort field ("name", "thc_percentage", "popularity")
- `order` (string, optional): Sort order ("asc", "desc")

**Example Request:**
```http
GET /api/strains?state=CA&type=indica&thc_min=20&limit=50&sort=popularity&order=desc
```

**Response:**
```json
{
  "success": true,
  "data": [
    {
      "strain_id": "strain_001",
      "name": "OG Kush",
      "type": "hybrid",
      "genetics": "Chemdawg × Lemon Thai × Pakistani Kush",
      "thc_percentage": 24.5,
      "cbd_percentage": 0.8,
      "dominant_terpene": "limonene",
      "terpene_profile": {
        "limonene": 1.2,
        "myrcene": 0.8,
        "pinene": 0.5,
        "linalool": 0.3
      },
      "effects": ["relaxed", "happy", "euphoric"],
      "flavors": ["earthy", "pine", "citrus"],
      "medical_uses": ["stress", "pain", "insomnia"],
      "flowering_time": 63,
      "yield": "medium",
      "growing_difficulty": "moderate",
      "popularity_score": 9.2,
      "created_at": "2025-01-15T10:30:00Z",
      "updated_at": "2025-11-06T10:30:00Z"
    }
  ],
  "meta": {
    "total_count": 1250,
    "page_count": 25,
    "current_page": 1,
    "per_page": 50
  }
}
```

### Get Strain by ID

```http
GET /api/strains/{strain_id}
```

**Path Parameters:**
- `strain_id` (string, required): Unique strain identifier

**Response:**
```json
{
  "success": true,
  "data": {
    "strain_id": "strain_001",
    "name": "OG Kush",
    "type": "hybrid",
    "genetics": "Chemdawg × Lemon Thai × Pakistani Kush",
    "breeder": "Imperial Genetics",
    "thc_percentage": 24.5,
    "cbd_percentage": 0.8,
    "thcv_percentage": 0.2,
    "cbg_percentage": 1.1,
    "dominant_terpene": "limonene",
    "terpene_profile": {
      "limonene": 1.2,
      "myrcene": 0.8,
      "pinene": 0.5,
      "linalool": 0.3,
      "caryophyllene": 0.4,
      "humulene": 0.2
    },
    "effects": ["relaxed", "happy", "euphoric", "sleepy"],
    "flavors": ["earthy", "pine", "citrus", "diesel"],
    "medical_uses": ["stress", "pain", "insomnia", "appetite"],
    "side_effects": ["dry_mouth", "dry_eyes"],
    "flowering_time": 63,
    "yield": "medium",
    "growing_difficulty": "moderate",
    "indoor_outdoor": "both",
    "plant_height": "medium",
    "popularity_score": 9.2,
    "review_count": 1847,
    "average_rating": 4.3,
    "lab_results": {
      "lab_name": "SC Labs",
      "test_date": "2025-10-15",
      "pesticides": "pass",
      "heavy_metals": "pass",
      "microbials": "pass"
    },
    "created_at": "2025-01-15T10:30:00Z",
    "updated_at": "2025-11-06T10:30:00Z"
  }
}
```

### Search Strains

```http
GET /api/strains/search
```

**Query Parameters:**
- `q` (string, required): Search query
- `state` (string, optional): Filter by state
- `limit` (integer, optional): Number of results (default: 20, max: 100)
- `fuzzy` (boolean, optional): Enable fuzzy matching (default: true)

**Example Request:**
```http
GET /api/strains/search?q=purple kush&state=CA&limit=10
```

**Response:**
```json
{
  "success": true,
  "data": [
    {
      "strain_id": "strain_042",
      "name": "Purple Kush",
      "type": "indica",
      "thc_percentage": 22.0,
      "cbd_percentage": 0.5,
      "match_score": 0.95,
      "highlight": {
        "name": "<em>Purple Kush</em>",
        "description": "Pure indica strain with <em>purple</em> hues"
      }
    }
  ]
}
```

### Get Strain Recommendations

```http
POST /api/strains/recommendations
```

**Request Body:**
```json
{
  "user_preferences": {
    "effects": ["relaxed", "happy", "creative"],
    "flavors": ["citrus", "sweet"],
    "avoid_effects": ["paranoid", "anxious"],
    "thc_preference": "medium",
    "cbd_preference": "low",
    "consumption_method": "smoking",
    "experience_level": "intermediate"
  },
  "medical_conditions": ["chronic_pain", "insomnia"],
  "location": {
    "state": "CA",
    "latitude": 34.0522,
    "longitude": -118.2437,
    "radius_miles": 25
  },
  "limit": 10
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "recommendations": [
      {
        "strain": {
          "strain_id": "strain_001",
          "name": "OG Kush",
          "type": "hybrid",
          "thc_percentage": 24.5,
          "cbd_percentage": 0.8
        },
        "match_score": 0.92,
        "match_reasons": [
          "High match for relaxed effect",
          "Compatible with citrus flavor preference",
          "Suitable THC level for intermediate user"
        ],
        "availability": {
          "dispensaries_count": 12,
          "avg_price": 45.00,
          "in_stock": true
        }
      }
    ],
    "recommendation_id": "rec_abc123"
  }
}
```

## 🏪 Dispensary Endpoints

### Get Dispensaries

```http
GET /api/dispensaries
```

**Query Parameters:**
- `state` (string, required): State abbreviation
- `latitude` (float, optional): Latitude for location-based search
- `longitude` (float, optional): Longitude for location-based search
- `radius` (float, optional): Search radius in miles (default: 25, max: 100)
- `license_type` (string, optional): "medical", "recreational", "both"
- `delivery` (boolean, optional): Filter by delivery availability
- `pickup` (boolean, optional): Filter by pickup availability
- `limit` (integer, optional): Number of results (default: 50)

**Example Request:**
```http
GET /api/dispensaries?state=CA&latitude=34.0522&longitude=-118.2437&radius=10&delivery=true
```

**Response:**
```json
{
  "success": true,
  "data": [
    {
      "dispensary_id": "disp_001",
      "name": "Green Valley Dispensary",
      "slug": "green-valley-dispensary",
      "state": "CA",
      "city": "Los Angeles",
      "address": "123 Cannabis St, Los Angeles, CA 90210",
      "latitude": 34.0522,
      "longitude": -118.2437,
      "phone": "+1-555-420-1234",
      "website": "https://greenvalley.com",
      "license_number": "CDPH-10001234",
      "license_type": "both",
      "delivery_available": true,
      "pickup_available": true,
      "curbside_available": true,
      "hours": {
        "monday": "09:00-21:00",
        "tuesday": "09:00-21:00",
        "wednesday": "09:00-21:00",
        "thursday": "09:00-21:00",
        "friday": "09:00-22:00",
        "saturday": "09:00-22:00",
        "sunday": "10:00-20:00"
      },
      "rating": 4.6,
      "review_count": 342,
      "distance_miles": 2.3,
      "compliance_status": "compliant",
      "created_at": "2024-01-15T10:30:00Z",
      "updated_at": "2025-11-06T10:30:00Z"
    }
  ]
}
```

### Get Dispensary by ID

```http
GET /api/dispensaries/{dispensary_id}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "dispensary_id": "disp_001",
    "name": "Green Valley Dispensary",
    "description": "Premium cannabis dispensary serving Los Angeles since 2018",
    "state": "CA",
    "city": "Los Angeles",
    "address": "123 Cannabis St, Los Angeles, CA 90210",
    "latitude": 34.0522,
    "longitude": -118.2437,
    "phone": "+1-555-420-1234",
    "email": "info@greenvalley.com",
    "website": "https://greenvalley.com",
    "social_media": {
      "instagram": "@greenvalleydispensary",
      "twitter": "@greenvalley",
      "facebook": "GreenValleyDispensary"
    },
    "license_number": "CDPH-10001234",
    "license_type": "both",
    "license_expiry": "2026-12-31",
    "delivery_available": true,
    "delivery_radius_miles": 15,
    "delivery_fee": 5.00,
    "delivery_minimum": 50.00,
    "pickup_available": true,
    "curbside_available": true,
    "hours": {
      "monday": "09:00-21:00",
      "tuesday": "09:00-21:00"
    },
    "payment_methods": ["cash", "debit", "credit", "cashless_atm"],
    "features": ["veteran_discount", "senior_discount", "first_time_discount"],
    "rating": 4.6,
    "review_count": 342,
    "compliance_status": "compliant",
    "last_inspection": "2025-10-15",
    "product_categories": ["flower", "edibles", "concentrates", "vapes", "topicals"],
    "brand_count": 45,
    "product_count": 287
  }
}
```

### Get Dispensary Products

```http
GET /api/dispensaries/{dispensary_id}/products
```

**Query Parameters:**
- `category` (string, optional): "flower", "edibles", "concentrates", "vapes", "topicals"
- `brand` (string, optional): Filter by brand name
- `strain_id` (string, optional): Filter by strain
- `in_stock` (boolean, optional): Filter by availability
- `price_min` (float, optional): Minimum price
- `price_max` (float, optional): Maximum price
- `sort` (string, optional): "price", "name", "thc", "popularity"
- `limit` (integer, optional): Number of results

**Response:**
```json
{
  "success": true,
  "data": [
    {
      "product_id": "prod_001",
      "dispensary_id": "disp_001",
      "name": "OG Kush - Premium Flower",
      "brand": "Top Shelf Gardens",
      "category": "flower",
      "subcategory": "indoor",
      "strain_id": "strain_001",
      "strain_name": "OG Kush",
      "description": "Premium indoor-grown OG Kush with exceptional terpene profile",
      "price": 45.00,
      "unit": "eighth",
      "weight_grams": 3.5,
      "thc_percentage": 24.5,
      "cbd_percentage": 0.8,
      "total_cannabinoids": 26.1,
      "terpene_profile": {
        "limonene": 1.2,
        "myrcene": 0.8
      },
      "in_stock": true,
      "stock_quantity": 15,
      "low_stock_threshold": 5,
      "lab_tested": true,
      "lab_results": {
        "lab_name": "SC Labs",
        "test_date": "2025-10-15",
        "batch_id": "GV-001-102525"
      },
      "harvest_date": "2025-10-01",
      "package_date": "2025-10-20",
      "images": [
        "https://cdn.weedhounds.com/products/prod_001_main.jpg",
        "https://cdn.weedhounds.com/products/prod_001_lab.jpg"
      ],
      "created_at": "2025-10-20T10:30:00Z",
      "updated_at": "2025-11-06T10:30:00Z"
    }
  ]
}
```

## 🧪 Terpene Analysis Endpoints

### Analyze Terpene Profile

```http
POST /api/terpenes/analyze
```

**Request Body:**
```json
{
  "strain_id": "strain_001",
  "terpene_data": {
    "limonene": 1.2,
    "myrcene": 0.8,
    "pinene": 0.5,
    "linalool": 0.3,
    "caryophyllene": 0.4,
    "humulene": 0.2,
    "terpinolene": 0.1,
    "ocimene": 0.1
  },
  "analysis_type": "effects_prediction"
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "analysis_id": "terp_analysis_001",
    "strain_id": "strain_001",
    "terpene_profile": {
      "total_terpenes": 3.6,
      "dominant_terpene": "limonene",
      "terpene_percentages": {
        "limonene": 33.3,
        "myrcene": 22.2,
        "pinene": 13.9,
        "caryophyllene": 11.1,
        "linalool": 8.3,
        "humulene": 5.6,
        "terpinolene": 2.8,
        "ocimene": 2.8
      }
    },
    "predicted_effects": {
      "primary_effects": [
        {
          "effect": "uplifting",
          "confidence": 0.89,
          "terpene_contributors": ["limonene", "pinene"]
        },
        {
          "effect": "relaxing",
          "confidence": 0.76,
          "terpene_contributors": ["myrcene", "linalool"]
        }
      ],
      "potential_side_effects": [
        {
          "effect": "dry_mouth",
          "probability": 0.23
        }
      ]
    },
    "flavor_profile": {
      "primary_flavors": ["citrus", "pine", "earthy"],
      "secondary_flavors": ["floral", "spicy"],
      "flavor_intensity": 7.8
    },
    "medical_potential": [
      {
        "condition": "anxiety",
        "potential": "high",
        "supporting_terpenes": ["limonene", "linalool"]
      },
      {
        "condition": "inflammation",
        "potential": "medium",
        "supporting_terpenes": ["caryophyllene", "humulene"]
      }
    ],
    "analysis_metadata": {
      "processing_time_ms": 45,
      "model_version": "2.1.0",
      "confidence_score": 0.87
    }
  }
}
```

### Get Terpene Effects Prediction

```http
POST /api/terpenes/effects
```

**Request Body:**
```json
{
  "terpene_profile": {
    "limonene": 1.2,
    "myrcene": 0.8,
    "pinene": 0.5,
    "linalool": 0.3
  },
  "user_context": {
    "tolerance": "medium",
    "consumption_method": "vaping",
    "time_of_day": "evening"
  }
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "effects_prediction": {
      "onset_time_minutes": 5,
      "duration_hours": 3,
      "intensity": "medium-high",
      "primary_effects": [
        {
          "effect": "relaxed",
          "probability": 0.85,
          "onset_minutes": 5,
          "duration_minutes": 120
        },
        {
          "effect": "euphoric",
          "probability": 0.72,
          "onset_minutes": 10,
          "duration_minutes": 90
        }
      ],
      "cognitive_effects": {
        "focus": 0.3,
        "creativity": 0.6,
        "motivation": 0.4
      },
      "physical_effects": {
        "body_high": 0.8,
        "sedation": 0.6,
        "pain_relief": 0.7
      }
    },
    "recommendations": {
      "optimal_dosage": "low to medium",
      "best_time": "evening",
      "consumption_tips": [
        "Start with small amounts due to high myrcene content",
        "Best consumed 2-3 hours before bedtime"
      ]
    }
  }
}
```

### Compare Terpene Profiles

```http
POST /api/terpenes/compare
```

**Request Body:**
```json
{
  "profile_a": {
    "strain_id": "strain_001",
    "terpenes": {
      "limonene": 1.2,
      "myrcene": 0.8,
      "pinene": 0.5
    }
  },
  "profile_b": {
    "strain_id": "strain_002",
    "terpenes": {
      "limonene": 0.9,
      "myrcene": 1.1,
      "linalool": 0.7
    }
  }
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "similarity_score": 0.73,
    "comparison": {
      "common_terpenes": ["limonene", "myrcene"],
      "unique_to_a": ["pinene"],
      "unique_to_b": ["linalool"],
      "dominant_differences": {
        "profile_a_dominant": "limonene",
        "profile_b_dominant": "myrcene"
      }
    },
    "effect_differences": {
      "more_energizing": "profile_a",
      "more_relaxing": "profile_b",
      "difference_magnitude": "moderate"
    }
  }
}
```

## 💰 Pricing Endpoints

### Get Price Comparison

```http
GET /api/pricing/compare
```

**Query Parameters:**
- `strain_id` (string, optional): Compare prices for specific strain
- `product_name` (string, optional): Search by product name
- `category` (string, optional): Filter by product category
- `state` (string, required): State for price comparison
- `latitude` (float, optional): User location for distance calculation
- `longitude` (float, optional): User location for distance calculation
- `radius` (float, optional): Search radius in miles

**Response:**
```json
{
  "success": true,
  "data": {
    "product_name": "OG Kush",
    "category": "flower",
    "strain_id": "strain_001",
    "price_comparison": [
      {
        "dispensary_id": "disp_001",
        "dispensary_name": "Green Valley Dispensary",
        "price": 45.00,
        "unit": "eighth",
        "price_per_gram": 12.86,
        "in_stock": true,
        "distance_miles": 2.3,
        "delivery_available": true,
        "pickup_available": true,
        "product_id": "prod_001"
      },
      {
        "dispensary_id": "disp_002",
        "dispensary_name": "Cannabis Corner",
        "price": 42.00,
        "unit": "eighth",
        "price_per_gram": 12.00,
        "in_stock": true,
        "distance_miles": 5.1,
        "delivery_available": false,
        "pickup_available": true,
        "product_id": "prod_045"
      }
    ],
    "price_statistics": {
      "lowest_price": 38.00,
      "highest_price": 55.00,
      "average_price": 46.50,
      "median_price": 45.00,
      "price_range": 17.00,
      "dispensaries_compared": 8
    },
    "recommendations": {
      "best_value": {
        "dispensary_id": "disp_002",
        "reason": "Lowest price per gram within 10 miles"
      },
      "closest_option": {
        "dispensary_id": "disp_001",
        "reason": "Closest dispensary with competitive pricing"
      }
    }
  }
}
```

## 👤 User Endpoints

### Get User Profile

```http
GET /api/users/profile
```

**Response:**
```json
{
  "success": true,
  "data": {
    "user_id": "user_001",
    "username": "cannabis_enthusiast",
    "email": "user@example.com",
    "first_name": "John",
    "last_name": "Doe",
    "date_of_birth": "1985-06-15",
    "state": "CA",
    "preferences": {
      "favorite_effects": ["relaxed", "creative", "happy"],
      "avoid_effects": ["paranoid", "anxious"],
      "preferred_thc_range": [15, 25],
      "preferred_cbd_range": [0, 5],
      "favorite_consumption_methods": ["vaping", "edibles"],
      "experience_level": "intermediate"
    },
    "medical_profile": {
      "has_medical_card": true,
      "medical_conditions": ["chronic_pain", "insomnia"],
      "allergies": ["nuts"],
      "medications": []
    },
    "activity_stats": {
      "strains_tried": 47,
      "reviews_written": 12,
      "favorite_strains_count": 8,
      "last_activity": "2025-11-06T09:15:00Z"
    },
    "created_at": "2024-03-15T10:30:00Z",
    "updated_at": "2025-11-06T10:30:00Z"
  }
}
```

### Update User Preferences

```http
PUT /api/users/preferences
```

**Request Body:**
```json
{
  "favorite_effects": ["relaxed", "creative", "focused"],
  "avoid_effects": ["paranoid", "dry_mouth"],
  "preferred_thc_range": [18, 28],
  "preferred_cbd_range": [0, 3],
  "favorite_consumption_methods": ["vaping", "dabbing"],
  "experience_level": "advanced",
  "preferred_terpenes": ["limonene", "pinene", "linalool"]
}
```

### Get User Recommendations History

```http
GET /api/users/recommendations
```

**Query Parameters:**
- `limit` (integer, optional): Number of results (default: 20)
- `offset` (integer, optional): Pagination offset

**Response:**
```json
{
  "success": true,
  "data": [
    {
      "recommendation_id": "rec_001",
      "created_at": "2025-11-06T10:30:00Z",
      "strains_recommended": 5,
      "user_feedback": {
        "tried_strain": "strain_001",
        "rating": 4,
        "effects_experienced": ["relaxed", "happy"],
        "would_recommend": true
      }
    }
  ]
}
```

## 📊 Analytics Endpoints

### Get Platform Statistics

```http
GET /api/analytics/stats
```

**Query Parameters:**
- `timeframe` (string, optional): "24h", "7d", "30d", "90d" (default: "7d")
- `metric` (string, optional): Specific metric to retrieve

**Response:**
```json
{
  "success": true,
  "data": {
    "timeframe": "7d",
    "strain_analytics": {
      "total_strains": 12847,
      "most_searched": [
        {"strain_name": "OG Kush", "search_count": 2456},
        {"strain_name": "Blue Dream", "search_count": 2134}
      ],
      "trending_strains": [
        {"strain_name": "Purple Runtz", "growth_rate": 156}
      ]
    },
    "terpene_analytics": {
      "analyses_performed": 8934,
      "most_analyzed_terpene": "limonene",
      "accuracy_rate": 0.94
    },
    "dispensary_analytics": {
      "active_dispensaries": 1247,
      "products_tracked": 89534,
      "price_updates": 23456
    },
    "user_activity": {
      "active_users": 45678,
      "searches_performed": 123456,
      "recommendations_generated": 34567
    }
  }
}
```

## 🔧 System Endpoints

### Health Check

```http
GET /health
```

**Response:**
```json
{
  "success": true,
  "data": {
    "status": "healthy",
    "timestamp": "2025-11-06T10:30:00Z",
    "version": "2.0.0",
    "environment": "production",
    "uptime_seconds": 86400,
    "checks": {
      "database": "healthy",
      "redis": "healthy",
      "gpu": "healthy",
      "external_apis": "healthy"
    }
  }
}
```

### System Metrics

```http
GET /api/metrics
```

**Response:**
```json
{
  "success": true,
  "data": {
    "performance": {
      "avg_response_time_ms": 45,
      "requests_per_second": 1247,
      "cache_hit_ratio": 0.96,
      "error_rate": 0.001
    },
    "system": {
      "cpu_usage_percent": 23.5,
      "memory_usage_percent": 67.8,
      "disk_usage_percent": 45.2,
      "active_connections": 342
    },
    "gpu": {
      "gpu_utilization_percent": 78.9,
      "gpu_memory_usage_percent": 45.6,
      "terpene_analyses_per_second": 156
    },
    "cannabis_metrics": {
      "strain_searches_per_minute": 89,
      "recommendations_per_minute": 23,
      "price_comparisons_per_minute": 45
    }
  }
}
```

## 🚨 Error Codes

| Code | Message | Description |
|------|---------|-------------|
| 400 | Bad Request | Invalid request parameters |
| 401 | Unauthorized | Authentication required |
| 403 | Forbidden | Insufficient permissions |
| 404 | Not Found | Resource not found |
| 422 | Validation Error | Invalid input data |
| 429 | Rate Limited | Too many requests |
| 500 | Internal Error | Server error |
| 503 | Service Unavailable | System maintenance |

### Error Response Format

```json
{
  "success": false,
  "data": null,
  "meta": {
    "timestamp": "2025-11-06T10:30:00Z",
    "request_id": "req_abc123",
    "processing_time_ms": 12
  },
  "errors": [
    {
      "code": "STRAIN_NOT_FOUND",
      "message": "Strain with ID 'invalid_id' not found",
      "field": "strain_id",
      "details": {
        "provided_value": "invalid_id",
        "valid_format": "strain_[0-9]+"
      }
    }
  ]
}
```

## 📝 Rate Limiting

API requests are rate-limited based on authentication status and endpoint:

| Endpoint Category | Authenticated | Unauthenticated |
|------------------|---------------|-----------------|
| Search | 1000/hour | 100/hour |
| Strain Details | 5000/hour | 500/hour |
| Terpene Analysis | 500/hour | 50/hour |
| User Operations | 2000/hour | N/A |
| Analytics | 100/hour | 10/hour |

Rate limit headers are included in all responses:

```http
X-RateLimit-Limit: 1000
X-RateLimit-Remaining: 995
X-RateLimit-Reset: 1699264200
```

## 🔄 Webhooks

Subscribe to real-time updates for various events:

### Available Events

- `strain.created` - New strain added
- `strain.updated` - Strain information updated
- `price.changed` - Product price updated
- `dispensary.created` - New dispensary added
- `terpene.analyzed` - Terpene analysis completed

### Webhook Configuration

```http
POST /api/webhooks
```

**Request Body:**
```json
{
  "url": "https://your-app.com/webhooks/cannabis",
  "events": ["strain.created", "price.changed"],
  "secret": "your-webhook-secret"
}
```

### Webhook Payload Example

```json
{
  "event": "strain.created",
  "timestamp": "2025-11-06T10:30:00Z",
  "data": {
    "strain_id": "strain_001",
    "name": "New Amazing Strain",
    "type": "hybrid"
  },
  "webhook_id": "webhook_001"
}
```

---

🌿 **Ready to build amazing cannabis applications!** This API provides everything you need to create powerful, compliant, and user-friendly cannabis platforms.
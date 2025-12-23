from fastapi import APIRouter, Depends
from data.chart_week import is_tracking_open

router = APIRouter()
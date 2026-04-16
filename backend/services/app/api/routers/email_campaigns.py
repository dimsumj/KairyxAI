from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status

from app.api.schemas.email_campaigns import EmailCampaignCreateRequest, EmailCampaignResponse, EmailCampaignUpdateRequest
from app.application.braze_provider import BrazeApiError
from app.application.email_campaigns import EmailCampaignService
from app.application.sendgrid_provider import SendGridApiError
from app.core.deps import get_email_campaign_service
from app.core.governance import ensure_permission, get_governance_context


router = APIRouter(prefix="/email-campaigns", tags=["email-campaigns"])


@router.get("", response_model=dict)
def list_email_campaigns(
    request: Request,
    status_filter: str | None = Query(default=None, alias="status"),
    service: EmailCampaignService = Depends(get_email_campaign_service),
):
    ensure_permission(get_governance_context(request), "email_campaigns.read")
    return {"items": service.list_campaigns(status=status_filter)}


@router.post("", response_model=EmailCampaignResponse, status_code=status.HTTP_201_CREATED)
def create_email_campaign(
    payload: EmailCampaignCreateRequest,
    request: Request,
    service: EmailCampaignService = Depends(get_email_campaign_service),
):
    ensure_permission(get_governance_context(request), "email_campaigns.write")
    try:
        return service.create_campaign(payload.model_dump(exclude_none=True))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Audience source '{exc.args[0]}' was not found.")
    except BrazeApiError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc))
    except SendGridApiError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.get("/{email_campaign_id}", response_model=EmailCampaignResponse)
def get_email_campaign(
    email_campaign_id: str,
    request: Request,
    service: EmailCampaignService = Depends(get_email_campaign_service),
):
    ensure_permission(get_governance_context(request), "email_campaigns.read")
    campaign = service.get_campaign(email_campaign_id)
    if campaign is None:
        raise HTTPException(status_code=404, detail=f"Email campaign '{email_campaign_id}' not found.")
    return campaign


@router.patch("/{email_campaign_id}", response_model=EmailCampaignResponse)
def update_email_campaign(
    email_campaign_id: str,
    payload: EmailCampaignUpdateRequest,
    request: Request,
    service: EmailCampaignService = Depends(get_email_campaign_service),
):
    ensure_permission(get_governance_context(request), "email_campaigns.write")
    try:
        return service.update_campaign(email_campaign_id, payload.model_dump(exclude_none=True))
    except KeyError as exc:
        detail = (
            f"Email campaign '{email_campaign_id}' not found."
            if str(exc.args[0]) == email_campaign_id
            else f"Audience source '{exc.args[0]}' was not found."
        )
        raise HTTPException(status_code=404, detail=detail)
    except BrazeApiError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc))
    except SendGridApiError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.post("/{email_campaign_id}/send-now", response_model=EmailCampaignResponse)
def send_email_campaign_now(
    email_campaign_id: str,
    request: Request,
    service: EmailCampaignService = Depends(get_email_campaign_service),
):
    ensure_permission(get_governance_context(request), "email_campaigns.execute")
    try:
        return service.send_now(email_campaign_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Email campaign '{email_campaign_id}' not found.")
    except BrazeApiError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc))
    except SendGridApiError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.post("/{email_campaign_id}/cancel", response_model=EmailCampaignResponse)
def cancel_email_campaign(
    email_campaign_id: str,
    request: Request,
    service: EmailCampaignService = Depends(get_email_campaign_service),
):
    ensure_permission(get_governance_context(request), "email_campaigns.write")
    try:
        return service.cancel_campaign(email_campaign_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Email campaign '{email_campaign_id}' not found.")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.delete("/{email_campaign_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_email_campaign(
    email_campaign_id: str,
    request: Request,
    service: EmailCampaignService = Depends(get_email_campaign_service),
):
    ensure_permission(get_governance_context(request), "email_campaigns.write")
    try:
        deleted = service.delete_campaign(email_campaign_id)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Email campaign '{email_campaign_id}' not found.")
    return Response(status_code=status.HTTP_204_NO_CONTENT)

from enum import Enum, unique

@unique
class Status(Enum):
    REVIEWED = 'reviewed'
    AWAITING_APPROVAL = 'awaiting_approval'
    APPROVED = 'approved'
    REJECTED = 'rejected'
